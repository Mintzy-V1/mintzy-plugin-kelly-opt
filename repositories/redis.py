"""Redis client for RMS exit signals + per-ticker exit status sync."""
import json
import os

from core.logging import get_logger
from state import sessions_store, trading_status

_db_log = get_logger("DATABASE")


# ---- Redis (used to receive per-ticker RMS exit signals from the worker process) ----
try:
    import redis as _redis

    _redis_host_raw = os.environ.get(
        "REDIS_HOST",
        "clustercfg.mintzy-redis.ci2qc0.use1.cache.amazonaws.com:6379",
    )
    _redis_host = _redis_host_raw
    _redis_port = int(os.environ.get("REDIS_PORT", "6379"))
    if ":" in _redis_host_raw:
        _redis_host, _redis_port_raw = _redis_host_raw.rsplit(":", 1)
        if _redis_port_raw:
            _redis_port = int(_redis_port_raw)

    _rms_redis = _redis.RedisCluster(
        host=_redis_host,
        port=_redis_port,
        ssl=True,
        ssl_cert_reqs=None,
        decode_responses=True,
        socket_connect_timeout=5,
        # Resilience fix (error_fix_detail.md #2): bound per-call socket ops too,
        # not just the initial connect, so a degraded connection can't hang forever.
        socket_timeout=float(os.environ.get("REDIS_SOCKET_TIMEOUT", "10")),
    )
    _rms_redis.ping()
except Exception as _e:
    _db_log.warning("RMS_REDIS_UNAVAILABLE error=%s", _e)
    _rms_redis = None


def drain_rms_exited_symbols(session_id: str) -> list:
    """
    Drain Redis list `autotrader:rms_exited:{sid}` populated by the trader's
    per-ticker RMS halt and remove those symbols from sessions_store. Idempotent.
    """
    if not _rms_redis or not session_id:
        return []
    from utils.redis_keys import rms_exited_key
    key = rms_exited_key(session_id)
    removed = []
    try:
        while True:
            raw = _rms_redis.lpop(key)
            if raw is None:
                break
            try:
                evt = json.loads(raw)
                sym = (evt.get("symbol") or "").upper()
            except Exception:
                sym = str(raw).upper()
            if not sym:
                continue
            sd = sessions_store.get(session_id)
            if sd and isinstance(sd.get("symbols"), list):
                sd["symbols"] = [s for s in sd["symbols"]
                                 if (s.get("symbol") if isinstance(s, dict) else s).upper() != sym]
            ts = trading_status.get(session_id)
            if ts and isinstance(ts.get("symbols"), list):
                ts["symbols"] = [s for s in ts["symbols"]
                                 if (s.get("symbol") if isinstance(s, dict) else s).upper() != sym]
            removed.append(sym)
            _db_log.info(
                "session=%s RMS_DRAINED symbol=%s remaining=%s",
                session_id, sym,
                [(s.get('symbol') if isinstance(s, dict) else s) for s in (sd.get('symbols') if sd else [])],
            )
    except Exception as e:
        _db_log.warning("session=%s RMS_DRAIN_FAILED error=%s", session_id, e)
    return removed


def sync_exit_status_from_redis(session_id: str):
    """
    Read worker-published EOD exit status from Redis
    (`autotrader:exit_status:{sid}`) into trading_status so
    /api/trading/exit-status stays accurate across processes.
    """
    if not _rms_redis or not session_id:
        return None
    from utils.redis_keys import exit_status_key
    key = exit_status_key(session_id)
    try:
        raw = _rms_redis.get(key)
        if not raw:
            return None
        data = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(data, dict):
            return None
        ts = trading_status.setdefault(session_id, {})
        if data.get("exit_initiated"):
            ts["exit_initiated"] = True
            if data.get("exit_time"):
                ts["exit_time"] = data.get("exit_time")
            if data.get("reason"):
                ts["exit_reason"] = data.get("reason")
            if ts.get("status") not in ("stopped", "completed_exit"):
                ts["status"] = "completed_exit"
        return data
    except Exception as e:
        _db_log.warning("session=%s EXIT_STATUS_SYNC_FAILED error=%s", session_id, e)
        return None
