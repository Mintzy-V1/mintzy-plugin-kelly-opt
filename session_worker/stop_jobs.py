import json
import time
from typing import Dict, Any, Optional

from utils.redis_keys import (
    pyramid_result_key,
    simulation_stop_key,
    stop_job_key,
)


class StopJobsMixin:

    @classmethod
    def _stop_job_key(cls, session_id: str) -> str:
        return stop_job_key(session_id)

    @classmethod
    def read_stop_job(cls, session_id: str) -> Optional[Dict[str, Any]]:
        try:
            raw = cls._redis().get(cls._stop_job_key(session_id))
            if not raw:
                return None
            return json.loads(raw)
        except Exception as e:
            print(f"[SessionManager] Failed to read stop job for {session_id}: {e}")
            return None

    @classmethod
    def init_stop_job(cls, session_id: str, **fields) -> Dict[str, Any]:
        job = {
            "session_id": session_id,
            "status": "stopping",
            "phase": "queued",
            "started_at": time.time(),
            "completed_at": None,
            "live_allowed": None,
            "pyramid": None,
            "error": None,
            "finalized": False,
            "response": None,
        }
        job.update(fields)
        try:
            cls._redis().setex(
                cls._stop_job_key(session_id),
                cls.STOP_JOB_TTL,
                json.dumps(job),
            )
            print(f"[SessionManager] Stop job initialized for {session_id} status={job['status']}")
        except Exception as e:
            print(f"[SessionManager] Failed to init stop job for {session_id}: {e}")
        return job

    @classmethod
    def update_stop_job(cls, session_id: str, **fields) -> Optional[Dict[str, Any]]:
        job = cls.read_stop_job(session_id) or {
            "session_id": session_id,
            "started_at": time.time(),
            "status": "stopping",
            "phase": "queued",
        }
        job.update(fields)
        try:
            cls._redis().setex(
                cls._stop_job_key(session_id),
                cls.STOP_JOB_TTL,
                json.dumps(job),
            )
        except Exception as e:
            print(f"[SessionManager] Failed to update stop job for {session_id}: {e}")
            return None
        return job

    @classmethod
    def complete_stop_job(
        cls,
        session_id: str,
        handoff: Dict[str, Any],
        *,
        simulation_stop: bool = True,
    ) -> None:
        cls.update_stop_job(
            session_id,
            status="completed",
            phase="done",
            completed_at=time.time(),
            live_allowed=bool(handoff.get("live_allowed", False)),
            pyramid=handoff,
            simulation_stop=simulation_stop,
            trading_status=(
                "simulation_stopped"
                if simulation_stop and bool(handoff.get("live_allowed", False))
                else "stopped"
            ),
        )
        print(
            f"[SessionManager] Stop job completed for {session_id} "
            f"live_allowed={handoff.get('live_allowed')}"
        )

    @classmethod
    def fail_stop_job(cls, session_id: str, error: str) -> None:
        cls.update_stop_job(
            session_id,
            status="failed",
            phase="failed",
            completed_at=time.time(),
            error=str(error)[:2000],
        )
        print(f"[SessionManager] Stop job failed for {session_id}: {error}")

    @classmethod
    def _pyramid_result_key(cls, session_id: str) -> str:
        return pyramid_result_key(session_id)

    @classmethod
    def read_pyramid_handoff_result(cls, session_id: str):
        try:
            raw = cls._redis().get(cls._pyramid_result_key(session_id))
            if not raw:
                return None
            return json.loads(raw)
        except Exception as e:
            print(f"[SessionManager] Failed to read pyramid handoff for {session_id}: {e}")
            return None

    @classmethod
    def clear_pyramid_handoff_result(cls, session_id: str) -> None:
        try:
            cls._redis().delete(cls._pyramid_result_key(session_id))
        except Exception as e:
            print(f"[SessionManager] Failed to clear pyramid handoff for {session_id}: {e}")

    @classmethod
    def _simulation_stop_key(cls, session_id: str) -> str:
        return simulation_stop_key(session_id)

    @classmethod
    def _mark_simulation_stop(cls, session_id: str) -> bool:
        try:
            cls._redis().setex(
                cls._simulation_stop_key(session_id),
                cls.SIMULATION_STOP_TTL,
                "1",
            )
            print(f"[SessionManager] Simulation stop flag set for {session_id}")
            return True
        except Exception as e:
            print(f"[SessionManager] Failed to set simulation stop flag: {e}")
            return False
