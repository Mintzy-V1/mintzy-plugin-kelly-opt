import os
import time
import json
import threading
import traceback
import signal
import multiprocessing as mp
from multiprocessing import Process
from queue import Empty
from typing import Dict, Any, Optional

from utils.redis_keys import (
    exit_request_key,
    exit_result_key,
    session_meta_key,
    session_pid_key,
)

from .constants import (
    LiveStartPrepResult,
    WORKER_FORCE_KILL_WAIT_SECONDS,
    WORKER_GRACEFUL_STOP_TIMEOUT_SECONDS,
    _PYRAMID_HANDOFF_BLOCKED,
)
from .worker_process import WorkerProcess
from .trader_worker import _trader_worker


class LifecycleMixin:

    @classmethod
    def _start_monitor(cls):
        """Start background thread to monitor worker health"""
        if cls._monitor_thread and cls._monitor_thread.is_alive():
            return

        def monitor_loop():
            print("[SessionManager] Health monitor started")
            while not cls._monitor_stop.is_set():
                try:
                    # Process health updates
                    while True:
                        try:
                            msg = cls._health_queue.get(timeout=0.1)
                            session_id = msg.get("session_id")
                            status = msg.get("status")

                            if session_id in cls._workers:
                                worker = cls._workers[session_id]
                                worker.update_heartbeat()

                                if status == "error":
                                    print(f"[SessionManager] Worker {session_id} reported error: {msg.get('error')}")
                                elif status == "stopped":
                                    print(f"[SessionManager] Worker {session_id} stopped gracefully")
                                elif status == "simulation_stopped":
                                    print(f"[SessionManager] Worker {session_id} simulation stopped (session auth kept)")

                        except Empty:
                            break

                    # Check for unhealthy workers
                    dead_workers = []
                    for session_id, worker in cls._workers.items():
                        if not worker.is_alive():
                            print(f"[SessionManager] Worker {session_id} process died")
                            dead_workers.append(session_id)
                        elif not worker.is_healthy(cls.WORKER_TIMEOUT):
                            print(f"[SessionManager] Worker {session_id} stopped responding (timeout)")
                            dead_workers.append(session_id)

                    # Clean up dead workers
                    for session_id in dead_workers:
                        cls._cleanup_worker(session_id)

                except Exception as e:
                    print(f"[SessionManager] Monitor error: {e}")

                time.sleep(cls.HEALTH_CHECK_INTERVAL)

        cls._monitor_stop.clear()
        cls._monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        cls._monitor_thread.start()

    @classmethod
    def prepare_for_live_start(
        cls, session_id: str, timeout: Optional[float] = None
    ) -> str:
        """
        Before spawning a live worker: clear paper (B) only.
        Never kills an existing live (non-B) worker — concurrent live starts are idempotent.
        """
        if timeout is None:
            timeout = WORKER_FORCE_KILL_WAIT_SECONDS

        cls._reconcile_session_registry(session_id)

        redis_pid = cls._get_redis_pid(session_id)
        meta = cls._get_session_worker_meta(session_id)
        local_worker = cls._workers.get(session_id)

        if redis_pid and cls._pid_alive(redis_pid):
            # The local record is authoritative when Redis meta was evicted — without
            # it an evicted key makes a live worker look like paper and it gets killed.
            if cls._session_has_live_worker(session_id):
                print(
                    f"[SessionManager] prepare_for_live_start: LIVE_ALREADY_RUNNING "
                    f"session={session_id} pid={redis_pid} "
                    f"strategy={(meta or {}).get('strategy') or (local_worker.strategy if local_worker else None)}"
                )
                return LiveStartPrepResult.LIVE_ALREADY_RUNNING

            strategy = (meta or {}).get("strategy")
            print(
                f"[SessionManager] prepare_for_live_start: force-killing paper/stale worker "
                f"session={session_id} pid={redis_pid} strategy={strategy or 'unknown'}"
            )
            if cls._terminate_session_worker(session_id, timeout):
                print(f"[SessionManager] prepare_for_live_start: PAPER_CLEARED session={session_id}")
                return LiveStartPrepResult.PAPER_CLEARED
            print(f"[SessionManager] prepare_for_live_start: NOT_CLEARED session={session_id}")
            return LiveStartPrepResult.NOT_CLEARED

        worker = cls._workers.get(session_id)
        if worker and worker.is_alive():
            if cls._session_has_live_worker(session_id):
                print(
                    f"[SessionManager] prepare_for_live_start: LIVE_ALREADY_RUNNING (local) "
                    f"session={session_id} pid={worker.process.pid}"
                )
                return LiveStartPrepResult.LIVE_ALREADY_RUNNING
            if cls._terminate_session_worker(session_id, timeout):
                return LiveStartPrepResult.PAPER_CLEARED
            return LiveStartPrepResult.NOT_CLEARED

        print(f"[SessionManager] prepare_for_live_start: CLEAR session={session_id}")
        return LiveStartPrepResult.CLEAR

    @classmethod
    def ensure_worker_stopped(
        cls, session_id: str, timeout: Optional[float] = None
    ) -> bool:
        """Force-clear paper/zombie workers for this session (never kills live workers)."""
        if timeout is None:
            timeout = WORKER_FORCE_KILL_WAIT_SECONDS

        cls._reconcile_session_registry(session_id)

        if cls._session_has_live_worker(session_id):
            print(
                f"[SessionManager] ensure_worker_stopped: live worker active for "
                f"{session_id} — refusing to terminate"
            )
            return False

        cleared = cls._terminate_session_worker(session_id, timeout)
        if not cleared:
            print(f"[SessionManager] ensure_worker_stopped: worker still alive for {session_id}")
            return False
        print(f"[SessionManager] ensure_worker_stopped: {session_id} is clear")
        return True

    @classmethod
    def start_session(cls, session_id: str, session_doc: dict, trading_logs_collection):
        """Start a new trading session in an isolated process"""
        gunicorn_pid = os.getpid()
        redis_pid_before = cls._get_redis_pid(session_id)
        worker_keys = list(cls._workers.keys())
        print(
            f"[SessionManager] start_session ENTER session={session_id} "
            f"gunicorn_pid={gunicorn_pid} local_workers={worker_keys} redis_pid={redis_pid_before}"
        )

        # Start monitor if not running
        cls._start_monitor()

        cls._reconcile_session_registry(session_id)

        # Check if session already exists
        if session_id in cls._workers:
            worker = cls._workers[session_id]
            if not worker.is_alive():
                print(f"[SessionManager] Stale worker registry for {session_id} — cleaning up")
                cls._cleanup_worker(session_id)
            else:
                print(f"[SessionManager] Session already running: {session_id}")
                raise RuntimeError(f"Session {session_id} is already running")

        redis_pid = cls._get_redis_pid(session_id)
        if redis_pid is not None and cls._pid_alive(redis_pid):
            print(f"[SessionManager] Session already running (Redis PID={redis_pid}): {session_id}")
            raise RuntimeError(f"Session {session_id} is already running")

        # Check worker limit
        worker_states = {
            sid: (w.process.pid, w.is_alive())
            for sid, w in cls._workers.items()
        }
        active_workers = sum(1 for _, alive in worker_states.values() if alive)
        print(
            f"[SessionManager] start_session worker check session={session_id} "
            f"active_workers={active_workers} max_workers={cls.MAX_WORKERS} "
            f"worker_states={worker_states}"
        )
        if active_workers >= cls.MAX_WORKERS:
            print(
                f"[SessionManager] start_session BLOCKED session={session_id} "
                f"MAX_WORKERS reached ({active_workers}/{cls.MAX_WORKERS})"
            )
            raise RuntimeError(
                f"Maximum concurrent sessions ({cls.MAX_WORKERS}) reached. "
                f"Stop a session or increase MAX_TRADER_WORKERS environment variable."
            )

        # Parse session configuration
        strategy = session_doc.get("strategy", "C")
        raw_symbols = session_doc.get("symbols", [])

        symbols = []
        allocations = {}

        for s in raw_symbols:
            if not isinstance(s, dict):
                print(
                    f"[SessionManager] start_session skipping non-dict symbol entry "
                    f"session={session_id} type={type(s).__name__} value={s!r}"
                )
                continue

            sym = s.get("symbol")
            try:
                cap = float(s.get("capital", 0))
                sl = float(s.get("stop_loss", 0.02))
            except (TypeError, ValueError) as parse_err:
                print(
                    f"[SessionManager] start_session symbol parse failed session={session_id} "
                    f"symbol={sym!r} entry={s!r}: {parse_err!r}"
                )
                raise

            if sym:
                symbols.append(sym)
                allocations[sym] = {
                    "capital": cap,
                    "stop_loss": sl
                }

        if not symbols:
            print(
                f"[SessionManager] start_session BLOCKED session={session_id} "
                f"empty symbols raw_count={len(raw_symbols)} raw_symbols={raw_symbols!r}"
            )
            raise RuntimeError("CRITICAL: Empty symbols list")

        print(f"[SessionManager] Starting session {session_id}")
        print(f"  Strategy: {strategy}")
        print(f"  Symbols: {symbols}")
        print(f"  Allocations: {allocations}")

        time_frame = session_doc.get("time_frame", "5 minutes")
        candle = session_doc.get("candle", "5m")
        configuration_id = session_doc.get("configuration_id")
        leverage_multiplier = session_doc.get("leverage_multiplier")
        if leverage_multiplier is not None:
            try:
                leverage_multiplier = float(leverage_multiplier)
                if leverage_multiplier <= 0:
                    leverage_multiplier = None
            except (TypeError, ValueError):
                leverage_multiplier = None
        if leverage_multiplier is not None:
            print(f"  Leverage multiplier: {leverage_multiplier}")

        # Prepare broker config
        broker_config = {
            "api_key": session_doc["api_key"],
            "client_code": session_doc["client_code"],
            "password": session_doc["password"],
            "broker_session": session_doc["broker_session"],
            "free_cash": session_doc.get("free_cash"),
        }

        # Get MongoDB connection details
        mongo_uri = os.environ.get(
            "MONGO_URI",
            "mongodb+srv://mintzy01ai_db_user:zTqQRkovgKbLXQdp@cluster0.cztcxpr.mongodb.net/?appName=Cluster0"
        )
        mongo_db_name = os.environ.get("MONGO_DB_NAME", "mintzy_plugin")
        mongo_config_db_name = os.environ.get("MONGO_CONFIG_DB_NAME", "test")

        # Create stop event and health queue for this worker
        stop_event = mp.Event()

        # Create worker process
        process = Process(
            target=_trader_worker,
            args=(
                session_id,
                strategy,
                symbols,
                allocations,
                time_frame,
                candle,
                broker_config,
                trading_logs_collection.name,
                stop_event,
                cls._health_queue,
                mongo_uri,
                mongo_db_name,
                configuration_id,
                mongo_config_db_name,
                leverage_multiplier,
            ),
            daemon=False  # Not daemon - we want proper cleanup
        )

        # Publish the strategy before the process exists. Writing it only after
        # process.start() leaves a window in which a concurrent live start reads no
        # meta, decides nothing is running, and spawns a second live worker.
        reservation_symbols = [
            (s or "").upper().replace("-EQ", "").strip()
            for s in symbols
            if s
        ]
        try:
            cls._redis().setex(
                session_meta_key(session_id),
                cls.SESSION_REDIS_TTL,
                json.dumps({
                    "strategy": strategy,
                    "pid": None,
                    "started_at": time.time(),
                    "symbols": reservation_symbols,
                    "reserved": True,
                }),
            )
        except Exception as reserve_err:
            print(f"[SessionManager] Meta reservation failed for {session_id}: {reserve_err}")

        # Start process
        try:
            process.start()
        except Exception as spawn_err:
            print(
                f"[SessionManager] process.start() failed session={session_id} "
                f"gunicorn_pid={gunicorn_pid}: {spawn_err!r}\n{traceback.format_exc()}"
            )
            try:
                cls._redis().delete(session_meta_key(session_id))
            except Exception:
                pass
            raise

        meta_saved = False
        last_redis_err = None
        for attempt in range(3):
            try:
                redis_client = cls._redis()
                if redis_client is None:
                    raise RuntimeError("Redis client unavailable")

                pid_key = session_pid_key(session_id)
                meta_key = session_meta_key(session_id)
                meta_symbols = [
                    (s or "").upper().replace("-EQ", "").strip()
                    for s in symbols
                    if s
                ]
                meta_payload = json.dumps({
                    "strategy": strategy,
                    "pid": process.pid,
                    "started_at": time.time(),
                    "symbols": meta_symbols,
                })
                pipe = redis_client.pipeline()
                pipe.setex(pid_key, cls.SESSION_REDIS_TTL, str(process.pid))
                pipe.setex(meta_key, cls.SESSION_REDIS_TTL, meta_payload)
                pipe.execute()
                meta_saved = True
                print(
                    f"[SessionManager] Redis mein save kiya — session={session_id} "
                    f"pid={process.pid} strategy={strategy} symbols={meta_symbols}"
                )
                break
            except Exception as e:
                last_redis_err = e
                if attempt < 2:
                    time.sleep(0.1 * (attempt + 1))

        if not meta_saved:
            e = last_redis_err or RuntimeError("Redis save failed")
            print(f"[SessionManager] CRITICAL: Redis save failed: {e}")
            strict_meta = os.environ.get("EOD_STRICT_REDIS_META", "false").lower() in (
                "1", "true", "yes",
            )
            if strict_meta and strategy != "B":
                print(
                    f"[SessionManager] EOD_STRICT_REDIS_META — terminating worker "
                    f"session={session_id} pid={process.pid}"
                )
                try:
                    if process.is_alive():
                        os.kill(process.pid, signal.SIGTERM)
                        process.join(timeout=10)
                        if process.is_alive():
                            process.terminate()
                            process.join(timeout=5)
                except Exception as kill_err:
                    print(f"[SessionManager] Worker terminate after Redis fail: {kill_err}")
                raise RuntimeError(
                    f"Redis meta save failed for live session {session_id}: {e}"
                )

        # Register worker
        worker = WorkerProcess(
            session_id, process, stop_event, cls._health_queue,
            strategy=strategy, symbols=reservation_symbols
        )
        cls._workers[session_id] = worker

        print(f"[SessionManager] Session {session_id} started in process {process.pid}")

        return {
            "session_id": session_id,
            "pid": process.pid,
            "started_at": worker.started_at
        }

    # @classmethod
    # def stop_session(cls, session_id: str):
    #     """Stop a trading session gracefully"""

    #     worker = cls._workers.get(session_id)

    #     if not worker:
    #         print(f"[SessionManager] No active session {session_id}")
    #         return False

    #     print(f"[SessionManager] Stopping session {session_id} (PID: {worker.process.pid})")

    #     # Signal worker to stop
    #     worker.stop_event.set()

    #     # Wait for graceful shutdown
    #     worker.process.join(timeout=15)

    #     # Force cleanup if still alive
    #     if worker.is_alive():
    #         print(f"[SessionManager] Force terminating session {session_id}")
    #         worker.process.terminate()
    #         worker.process.join(timeout=3)

    #         if worker.is_alive():
    #             worker.process.kill()

    #     # Remove from registry
    #     cls._workers.pop(session_id, None)

    #     print(f"[SessionManager] Session {session_id} stopped")
    #     return True


    @classmethod
    def stop_session_signal_only(cls, session_id: str) -> bool:
        """
        Signal the trading worker to stop without blocking for process exit.
        Safe to call from any gunicorn worker (local mp.Event or cross-worker SIGTERM).
        """
        print(f"[SessionManager] stop_session_signal_only: '{session_id}'")
        worker = cls._workers.get(session_id)

        if worker:
            print(f"[SessionManager] Signal-only local worker for {session_id}")
            worker.stop_event.set()
            return True

        try:
            pid_str = cls._redis().get(session_pid_key(session_id))
            if not pid_str:
                print(
                    f"[SessionManager] Signal-only: no local worker or Redis PID for {session_id} "
                    "— worker may already be stopped"
                )
                return True

            pid = int(pid_str)
            print(f"[SessionManager] Signal-only SIGTERM PID={pid} for {session_id}")
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                print(f"[SessionManager] Signal-only: PID={pid} already exited")
            return True
        except Exception as e:
            print(f"[SessionManager] Signal-only stop failed for {session_id}: {e}")
            return False

    @classmethod
    def stop_session(cls, session_id: str):
        print(f"[SessionManager] Stop request aaya: '{session_id}'")
        print(f"[SessionManager] Current workers: {list(cls._workers.keys())}")

        worker = cls._workers.get(session_id)

        # Ã¢â€ Â AGAR LOCAL MEMORY MEIN NAHI MILA Ã¢â‚¬â€ REDIS CHECK KARO
        if not worker:
            print(f"[SessionManager] Local memory mein nahi mila Ã¢â‚¬â€ Redis check kar raha hoon...")
            
            try:
                pid_str = cls._redis().get(session_pid_key(session_id))
                
                if not pid_str:
                    print(f"[SessionManager] Ã¢ÂÅ’ Redis mein bhi nahi mila Ã¢â‚¬â€ session already stopped hoga")
                    return False
                
                pid = int(pid_str)
                print(f"[SessionManager] Ã¢Å“â€¦ Redis mein mila Ã¢â‚¬â€ PID={pid} Ã¢â‚¬â€ kill kar raha hoon...")
                
                # Process ko SIGTERM bhejo Ã¢â‚¬â€ graceful shutdown
                try:
                    os.kill(pid, signal.SIGTERM)
                    print(f"[SessionManager] Ã¢Å“â€¦ SIGTERM bheja PID={pid} ko")
                    
                    # 30 sec wait karo graceful shutdown ke liye
                    try:
                        import psutil
                        proc = psutil.Process(pid)
                        proc.wait(timeout=60)
                        print(f"[SessionManager] Process {pid} gracefully band ho gaya")
                    except ImportError:
                        print("[SessionManager] psutil not installed — polling PID until exit")
                        if not cls._wait_for_pid_exit(pid, timeout=90):
                            print(f"[SessionManager] PID={pid} still alive after 90s — SIGKILL")
                            try:
                                os.kill(pid, signal.SIGKILL)
                            except ProcessLookupError:
                                pass
                            cls._wait_for_pid_exit(pid, timeout=10)
                    except Exception as wait_err:
                        if wait_err.__class__.__name__ == "TimeoutExpired":
                            print(f"[SessionManager] 60s baad bhi alive — SIGKILL bhej raha hoon...")
                            os.kill(pid, signal.SIGKILL)
                        elif wait_err.__class__.__name__ == "NoSuchProcess":
                            print(f"[SessionManager] Process {pid} already band ho gaya")
                        else:
                            print(f"[SessionManager] Process wait failed: {wait_err}")
                        
                except ProcessLookupError:
                    print(f"[SessionManager] Ã¢Å¡ Ã¯Â¸Â PID={pid} already exist nahi karta Ã¢â‚¬â€ already band tha")
                
                cls._delete_session_redis_keys(session_id)
                print(f"[SessionManager] Ã¢Å“â€¦ Session {session_id} stopped via Redis")
                return True
                
            except Exception as e:
                print(f"[SessionManager] Ã¢ÂÅ’ Redis stop failed: {e}")
                return False

        # Ã¢â€ Â NORMAL FLOW Ã¢â‚¬â€ local memory mein mila
        print(f"[SessionManager] Local memory mein mila Ã¢â‚¬â€ normal shutdown...")
        
        worker.stop_event.set()
        # Resilience fix (error_fix_detail.md #8): raised from 60s to
        # WORKER_GRACEFUL_STOP_TIMEOUT_SECONDS so this doesn't SIGKILL the child
        # before its own bounded shutdown sequence (pyramid handoff calc + writes)
        # has a chance to finish. See constants.py for the full budget math.
        worker.process.join(timeout=WORKER_GRACEFUL_STOP_TIMEOUT_SECONDS)

        if worker.is_alive():
            print(f"[SessionManager] Ã¢Å¡ Ã¯Â¸Â Force terminating {session_id}")
            worker.process.terminate()
            worker.process.join(timeout=5)
            if worker.is_alive():
                worker.process.kill()
        
        cls._delete_session_redis_keys(session_id)
        cls._workers.pop(session_id, None)
        print(f"[SessionManager] Ã¢Å“â€¦ Session {session_id} stopped")
        return True
    @classmethod
    def stop_simulation_session_async(cls, session_id: str) -> bool:
        """
        Non-blocking simulation stop: set flags, signal worker, return immediately.
        Worker completes pyramid/shutdown in background and updates Redis stop job.
        """
        print(f"[SessionManager] Async simulation stop request: '{session_id}'")
        existing = cls.read_stop_job(session_id)
        if existing and existing.get("status") in ("stopping", "completed"):
            print(
                f"[SessionManager] Stop job already {existing.get('status')} for {session_id} "
                "— idempotent accept"
            )
            return True

        if not cls._mark_simulation_stop(session_id):
            print(
                f"[SessionManager] Simulation stop aborted for {session_id} "
                "— Redis simulation-stop flag was not set"
            )
            return False

        cls.init_stop_job(session_id, status="stopping", phase="signal_sent")

        if not cls.stop_session_signal_only(session_id):
            cls.fail_stop_job(session_id, "Failed to signal simulation worker")
            return False

        cls.update_stop_job(session_id, phase="waiting_worker")
        return True

    @classmethod
    def stop_simulation_session(cls, session_id: str):
        """
        Blocking simulation stop (legacy / ?wait=true).
        Sets a Redis flag so the worker exit handler keeps status=authenticated in Mongo.
        """
        print(f"[SessionManager] Simulation stop request: '{session_id}'")
        if not cls._mark_simulation_stop(session_id):
            print(
                f"[SessionManager] Simulation stop aborted for {session_id} "
                "— Redis simulation-stop flag was not set"
            )
            return False
        cls.init_stop_job(session_id, status="stopping", phase="blocking_stop")
        stopped = cls.stop_session(session_id)
        cls._cleanup_worker(session_id)
        if stopped:
            fully_stopped = cls.ensure_worker_stopped(session_id)
            if not fully_stopped:
                print(f"[SessionManager] Simulation worker for {session_id} did not exit in time")
                cls.fail_stop_job(session_id, "Worker did not exit in time")
                return False
        if stopped:
            handoff = cls.read_pyramid_handoff_result(session_id) or dict(_PYRAMID_HANDOFF_BLOCKED)
            cls.complete_stop_job(session_id, handoff, simulation_stop=True)
        return stopped

    @classmethod
    def exit_symbol_for_session(cls, session_id: str, symbol: str) -> dict:
        """
        Send a single-symbol exit request to the trader worker process via Redis queue.
        Works across gunicorn workers because Redis is shared.
        """
        print(f"[SessionManager] Exit request: session={session_id} symbol={symbol}")
        
        # Verify session exists (local or Redis)
        worker = cls._workers.get(session_id)
        if not worker:
            # Check Redis for PID (cross-worker case)
            try:
                pid_str = cls._redis().get(session_pid_key(session_id))
                if not pid_str:
                    return {
                        "success": False,
                        "symbol": symbol,
                        "message": f"Session {session_id} not found (not running)"
                    }
            except Exception as e:
                return {
                    "success": False,
                    "symbol": symbol,
                    "message": f"Redis error checking session: {e}"
                }
        
        # Push exit request to Redis queue
        try:
            exit_queue_key = exit_request_key(session_id)
            payload = json.dumps({"symbol": symbol.upper()})
            cls._redis().rpush(exit_queue_key, payload)
            # Set TTL on the queue key so it auto-cleans (5 minutes)
            cls._redis().expire(exit_queue_key, 300)
            
            print(f"[SessionManager] Ã¢Å“â€¦ Exit request pushed to Redis for {symbol}")
            
            # Wait briefly for the result (max 10 seconds)
            result_key = exit_result_key(session_id, symbol)
            for _ in range(20):  # 20 Ãƒâ€” 0.5s = 10s
                time.sleep(0.5)
                result_raw = cls._redis().get(result_key)
                if result_raw:
                    cls._redis().delete(result_key)  # cleanup
                    return json.loads(result_raw)
            
            # Timeout Ã¢â‚¬â€ request was sent but no result yet
            return {
                "success": True,
                "symbol": symbol,
                "message": f"Exit request sent for {symbol}. Order is being processed (reconciliation will handle it)."
            }
            
        except Exception as e:
            return {
                "success": False,
                "symbol": symbol,
                "message": f"Failed to send exit request: {e}"
            }


    @classmethod
    def get_session_status(cls, session_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a trading session"""

        worker = cls._workers.get(session_id)

        if not worker:
            return None

        return {
            "session_id": session_id,
            "pid": worker.process.pid,
            "is_alive": worker.is_alive(),
            "is_healthy": worker.is_healthy(),
            "started_at": worker.started_at,
            "uptime": time.time() - worker.started_at,
            "last_heartbeat": worker.last_heartbeat
        }

    @classmethod
    def list_sessions(cls) -> Dict[str, Dict[str, Any]]:
        """List all active sessions"""
        return {
            session_id: cls.get_session_status(session_id)
            for session_id in cls._workers.keys()
        }

    @classmethod
    def stop_all_sessions(cls):
        """Stop all trading sessions"""

        print("[SessionManager] Stopping all sessions...")

        session_ids = list(cls._workers.keys())

        for session_id in session_ids:
            cls.stop_session(session_id)

        # Stop monitor
        cls._monitor_stop.set()
        if cls._monitor_thread:
            cls._monitor_thread.join(timeout=5)

        print("[SessionManager] All sessions stopped")
