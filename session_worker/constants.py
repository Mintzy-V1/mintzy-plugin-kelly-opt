import os

SIMULATION_STOP_PREFIX = "autotrader:simulation_stop:"
# Must outlive the whole shutdown sequence (trader join + shutdown + square-off).
# If it expires first the worker takes the plain-stop branch and marks the session
# "stopped", which de-authenticates it and blocks the live start.
SIMULATION_STOP_TTL = int(os.environ.get("SIMULATION_STOP_TTL", "1800"))
PYRAMID_RESULT_PREFIX = "autotrader:pyramid_result:"
PYRAMID_RESULT_TTL = 600
STOP_JOB_PREFIX = "autotrader:stop_job:"
STOP_JOB_TTL = 600

# Grace period for concurrent live-start when worker meta is missing (legacy / race).
LIVE_START_WORKER_GRACE_SECONDS = int(os.environ.get("LIVE_START_WORKER_GRACE_SECONDS", "60"))
# Max wait after SIGKILL when clearing paper/zombie workers (live-start prep path).
WORKER_FORCE_KILL_WAIT_SECONDS = float(os.environ.get("WORKER_FORCE_KILL_WAIT_SECONDS", "10"))

# Resilience fix (error_fix_detail.md #8): the child worker's own graceful-shutdown
# budget (trader_thread.join(45) + trader.shutdown()'s now-bounded broker/Mongo
# calls + trader_thread.join(60), see trader_worker.py) can legitimately take up
# to ~195s in the worst case (each broker call bounded at BROKER_API_TIMEOUT_SECONDS
# by broker_angle.py, up to 3 sequential calls in the pyramid cash lookup, plus the
# two thread joins). The parent's stop_session() previously gave up and SIGKILLed
# at 60s - well before that, killing the child mid pyramid-calculation and losing
# the handoff write. This raises the parent's patience comfortably above the
# child's real worst case; ordinary shutdowns still complete in seconds either way.
WORKER_GRACEFUL_STOP_TIMEOUT_SECONDS = float(
    os.environ.get("WORKER_GRACEFUL_STOP_TIMEOUT_SECONDS", "240")
)


class LiveStartPrepResult:
    """Outcome of prepare_for_live_start — do not kill an existing live worker."""
    CLEAR = "clear"
    PAPER_CLEARED = "paper_cleared"
    LIVE_ALREADY_RUNNING = "live_already_running"
    NOT_CLEARED = "not_cleared"

# Live handoff is blocked unless pyramid sets live_allowed=True explicitly.
_PYRAMID_HANDOFF_BLOCKED = {
    "applied": False,
    "live_allowed": False,
    "reason": "pyramid_not_run",
    "profitable_count": 0,
    "symbols_for_live": [],
}
