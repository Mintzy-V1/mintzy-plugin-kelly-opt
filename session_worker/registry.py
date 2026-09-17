import os
import time
import json
import signal
from typing import Dict, Any, Optional

from utils.redis_keys import (
    session_cleanup_keys,
    session_meta_key,
    session_pid_key,
)

from .constants import LIVE_START_WORKER_GRACE_SECONDS, WORKER_FORCE_KILL_WAIT_SECONDS


class RegistryMixin:

    @classmethod
    def _cleanup_worker(cls, session_id: str):
        """Clean up a dead or stopped worker"""
        worker = cls._workers.get(session_id)
        if not worker:
            return

        try:
            if worker.is_alive():
                worker.stop_event.set()
                worker.process.join(timeout=5)

                if worker.process.is_alive():
                    print(f"[SessionManager] Force terminating worker {session_id}")
                    worker.process.terminate()
                    worker.process.join(timeout=2)

                    if worker.process.is_alive():
                        worker.process.kill()
        except Exception as e:
            print(f"[SessionManager] Error cleaning up worker {session_id}: {e}")
        finally:
            cls._workers.pop(session_id, None)

    @classmethod
    def _pid_alive(cls, pid: int) -> bool:
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True

    @classmethod
    def _get_redis_pid(cls, session_id: str) -> Optional[int]:
        try:
            pid_str = cls._redis().get(session_pid_key(session_id))
            return int(pid_str) if pid_str else None
        except Exception:
            return None

    @classmethod
    def _session_meta_key(cls, session_id: str) -> str:
        return session_meta_key(session_id)

    @classmethod
    def _get_session_worker_meta(cls, session_id: str) -> Optional[Dict[str, Any]]:
        try:
            raw = cls._redis().get(cls._session_meta_key(session_id))
            if not raw:
                return None
            return json.loads(raw)
        except Exception:
            return None

    @classmethod
    def _set_session_worker_meta(cls, session_id: str, strategy: str, pid: int,
                                 symbols: list = None) -> None:
        meta = {
            "strategy": strategy,
            "pid": pid,
            "started_at": time.time(),
        }
        # session_symbols falls back to a broader set when this is absent, which
        # would widen the EOD square-off beyond this session.
        if symbols:
            meta["symbols"] = list(symbols)
        try:
            cls._redis().setex(
                cls._session_meta_key(session_id),
                cls.SESSION_REDIS_TTL,
                json.dumps(meta),
            )
        except Exception as e:
            print(f"[SessionManager] Redis meta save failed for {session_id}: {e}")

    @classmethod
    def _delete_session_redis_keys(cls, session_id: str) -> None:
        try:
            cls._redis().delete(*session_cleanup_keys(session_id))
        except Exception as e:
            print(f"[SessionManager] Redis delete failed for {session_id}: {e}")

    @classmethod
    def _is_live_worker_meta(cls, meta: Optional[Dict[str, Any]]) -> bool:
        if not meta:
            return False
        strategy = meta.get("strategy")
        if strategy and strategy != "B":
            return True
        started_at = meta.get("started_at")
        if strategy is None and started_at is not None:
            try:
                age = time.time() - float(started_at)
                if age < LIVE_START_WORKER_GRACE_SECONDS:
                    return True
            except (TypeError, ValueError):
                pass
        return False

    @classmethod
    def _session_has_live_worker(cls, session_id: str) -> bool:
        """True when a live (non-paper) trader worker must not be killed."""
        meta = cls._get_session_worker_meta(session_id)
        local_worker = cls._workers.get(session_id)
        local_is_live = bool(
            local_worker
            and local_worker.is_alive()
            and local_worker.strategy
            and local_worker.strategy != "B"
        )
        return cls._is_live_worker_meta(meta) or local_is_live

    @classmethod
    def _wait_for_pid_exit(cls, pid: int, timeout: float = 90.0) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if not cls._pid_alive(pid):
                return True
            time.sleep(0.5)
        return not cls._pid_alive(pid)

    @classmethod
    def _reconcile_session_registry(cls, session_id: str) -> None:
        """
        Drop stale local/Redis worker entries.
        Needed when stop-simulation runs on a different gunicorn worker than start.
        """
        worker = cls._workers.get(session_id)
        redis_pid = cls._get_redis_pid(session_id)

        if worker and not worker.is_alive():
            print(f"[SessionManager] Reconcile: dead local worker for {session_id}")
            cls._cleanup_worker(session_id)
            worker = None

        if worker and worker.is_alive():
            local_pid = worker.process.pid
            if redis_pid is None and worker.strategy and worker.strategy != "B":
                # A live worker outliving its Redis keys means the keys were lost,
                # not that a stop was requested. Restore them instead of killing it.
                print(
                    f"[SessionManager] Reconcile: live worker PID={local_pid} for {session_id} "
                    "has no Redis entry — republishing keys instead of terminating"
                )
                try:
                    cls._redis().setex(
                        session_pid_key(session_id), cls.SESSION_REDIS_TTL, str(local_pid)
                    )
                    cls._set_session_worker_meta(
                        session_id, worker.strategy, local_pid, symbols=worker.symbols
                    )
                except Exception as e:
                    print(f"[SessionManager] Reconcile: failed to republish keys for {session_id}: {e}")
            elif redis_pid is None:
                print(
                    f"[SessionManager] Reconcile: local worker PID={local_pid} for {session_id} "
                    "but Redis entry cleared (cross-worker stop) — cleaning up"
                )
                worker.stop_event.set()
                worker.process.join(timeout=5)
                if worker.is_alive():
                    try:
                        worker.process.terminate()
                        worker.process.join(timeout=3)
                    except Exception:
                        pass
                cls._cleanup_worker(session_id)
            elif redis_pid != local_pid:
                print(
                    f"[SessionManager] Reconcile: PID mismatch local={local_pid} redis={redis_pid} "
                    f"for {session_id} — cleaning up local registry"
                )
                cls._cleanup_worker(session_id)

        redis_pid = cls._get_redis_pid(session_id)
        if redis_pid is not None and not cls._pid_alive(redis_pid):
            print(f"[SessionManager] Reconcile: Redis PID={redis_pid} for {session_id} is dead — clearing")
            cls._delete_session_redis_keys(session_id)
            cls._workers.pop(session_id, None)

    @classmethod
    def _terminate_session_worker(
        cls, session_id: str, timeout: Optional[float] = None
    ) -> bool:
        """
        Force-kill paper/zombie session workers (local registry + Redis PID).
        Never kills a live (non-B) worker — returns False if one is active.
        """
        if timeout is None:
            timeout = WORKER_FORCE_KILL_WAIT_SECONDS

        if cls._session_has_live_worker(session_id):
            print(
                f"[SessionManager] _terminate_session_worker: refusing — live worker "
                f"active for {session_id}"
            )
            return False

        pids_to_kill: set[int] = set()
        worker = cls._workers.get(session_id)
        if worker:
            try:
                if worker.process.is_alive():
                    pids_to_kill.add(worker.process.pid)
            except Exception:
                pass
            cls._workers.pop(session_id, None)

        redis_pid = cls._get_redis_pid(session_id)
        if redis_pid is not None:
            pids_to_kill.add(redis_pid)

        cls._delete_session_redis_keys(session_id)

        for pid in pids_to_kill:
            if not cls._pid_alive(pid):
                continue
            print(
                f"[SessionManager] _terminate_session_worker: SIGKILL PID={pid} "
                f"for {session_id}"
            )
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

        for pid in pids_to_kill:
            if cls._pid_alive(pid) and not cls._wait_for_pid_exit(pid, timeout):
                print(
                    f"[SessionManager] _terminate_session_worker: PID={pid} still alive "
                    f"after {timeout}s for {session_id}"
                )

        cls._reconcile_session_registry(session_id)

        still_running = session_id in cls._workers
        if not still_running:
            for pid in pids_to_kill:
                if cls._pid_alive(pid):
                    still_running = True
                    break
        if not still_running:
            redis_pid = cls._get_redis_pid(session_id)
            still_running = redis_pid is not None and cls._pid_alive(redis_pid)

        return not still_running
