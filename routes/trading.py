"""Trading routes: live/paper start, stop, status, logs, snapshots, tradebooks."""
import csv
import io
import json
import logging
import os
import threading
import time
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Header, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from starlette.concurrency import run_in_threadpool

from core.config import LOGS_DIR
from core.logging import get_logger
from models import (
    TradingConfig,
    _sim_plugin_elapsed_ms,
    _sim_plugin_log,
    _sim_plugin_now_ms,
    _sim_plugin_payload_summary,
)
from repositories.db import (
    DB_CONNECTED,
    _trading_logs_mongo_query,
    add_log,
    exited_symbols_collection,
    fetch_logs_from_db,
    fetch_session_from_db,
    persist_session_metadata_sync,
    persist_session_state,
    pyramid_pnls_collection,
    trading_logs_collection,
)
from repositories.redis import _rms_redis, sync_exit_status_from_redis
from services.session_service import get_or_restore_session
from session_manager import LiveStartPrepResult, SessionManager
from state import sessions_store, trading_logs, trading_status
from trading_state import trading_snapshot

logger = logging.getLogger(__name__)
_trading_log = get_logger("TRADING")

router = APIRouter()


@router.get("/api/sessions/{session_id}/trades")
def get_session_trading_logs(
    session_id: str,
    simulation_logs: bool | None = Query(
        default=None,
        description="Filter by simulation (true) vs live (false). Omit for all logs.",
    ),
    x_plugin_api_key: str = Header(None),
):
    return list(
        trading_logs_collection.find(
            _trading_logs_mongo_query(session_id=session_id, simulation_logs=simulation_logs),
            {"_id": 0}
        ).sort("timestamp", 1)
    )


@router.get("/api/sessions/{session_id}/download")
def download_trading_logs(
    session_id: str,
    simulation_logs: bool | None = Query(
        default=None,
        description="Filter by simulation (true) vs live (false). Omit for all logs.",
    ),
):
    cursor = trading_logs_collection.find(
        _trading_logs_mongo_query(session_id=session_id, simulation_logs=simulation_logs),
        {"_id": 0}
    )

    output = io.StringIO()
    writer = None

    for doc in cursor:
        if writer is None:
            writer = csv.DictWriter(output, fieldnames=doc.keys())
            writer.writeheader()
        writer.writerow(doc)

    output.seek(0)

    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename={session_id}.csv"
        }
    )


@router.get("/api/trading/snapshot/{session_id}")
def get_trading_snapshot(session_id: str, x_plugin_api_key: str = Header(None)):
    snap = trading_snapshot.get(session_id)

    if not snap:
        return {
            "success": True,
            "ready": False,
            "message": "Snapshot initializing"
        }

    return {
        "success": True,
        "ready": True,
        "data": snap
    }


@router.get("/api/trading/live-pnl/{session_id}")
def get_live_pnl(session_id: str, x_plugin_api_key: str = Header(None)):
    """
    Returns tick-by-tick PnL for a running session.
    Data is written to Redis by the trader's on_ltp_tick() at most once per second.
    TTL is 5s — if the key is missing the trader is not running or WS is down.
    """
    if not _rms_redis:
        return {"success": False, "error": "Redis unavailable"}

    try:
        from utils.redis_keys import live_pnl_key
        raw = _rms_redis.get(live_pnl_key(session_id))
    except Exception as e:
        return {"success": False, "error": f"Redis error: {e}"}

    if not raw:
        return {
            "success": True,
            "ready": False,
            "message": "No live PnL data yet — trader may not be running or WS not connected",
        }

    try:
        data = json.loads(raw)
    except Exception:
        return {"success": False, "error": "Malformed payload in Redis"}

    return {"success": True, "ready": True, "data": data}


@router.post("/api/trading/start")
async def start_trading(config: TradingConfig, x_plugin_api_key: str = Header(None)):
    """
    Start automated trading with configured allocations.
    """
    is_simulation_start = config.strategy == "B"
    req_started_perf = time.perf_counter()
    req_started_ms = _sim_plugin_now_ms()

    if is_simulation_start:
        _sim_plugin_log(
            "start_trading ENTER (via start-simulation)",
            session_id=config.session_id,
            configuration_id=config.configuration_id,
            symbol_count=len(config.symbols),
            started_at_ms=req_started_ms,
            payload=_sim_plugin_payload_summary(config),
        )
    else:
        _trading_log.info("session=%s START_REQUEST strategy=%s", config.session_id, config.strategy)

    session_id = config.session_id
    restore_started_perf = time.perf_counter()
    session_data = await get_or_restore_session(session_id)
    if is_simulation_start:
        _sim_plugin_log(
            "start_trading phase=get_or_restore_session",
            session_id=session_id,
            elapsed_ms=_sim_plugin_elapsed_ms(restore_started_perf),
            session_status=session_data.get("status") if session_data else None,
            has_broker=bool(session_data and session_data.get("broker")),
            has_broker_session=bool(session_data and session_data.get("broker_session")),
        )

    symbols_payload = [
        {
            "symbol": s.symbol,
            "capital": float(s.capital),
            "stop_loss": float(s.stop_loss)
        }
        for s in config.symbols
    ]

    sessions_store[session_id]["strategy"] = config.strategy
    sessions_store[session_id]["symbols"] = symbols_payload
    if config.configuration_id:
        sessions_store[session_id]["configuration_id"] = config.configuration_id
    if config.leverage_multiplier is not None:
        sessions_store[session_id]["leverage_multiplier"] = float(config.leverage_multiplier)

    early_persist = {
        "strategy": config.strategy,
        "symbols": symbols_payload,
    }
    if config.configuration_id:
        early_persist["configuration_id"] = config.configuration_id
    if config.leverage_multiplier is not None:
        early_persist["leverage_multiplier"] = float(config.leverage_multiplier)
    persist_started_perf = time.perf_counter()
    await run_in_threadpool(persist_session_metadata_sync, session_id, early_persist)
    if is_simulation_start:
        _sim_plugin_log(
            "start_trading phase=persist_session_metadata_sync (early)",
            session_id=session_id,
            configuration_id=config.configuration_id,
            elapsed_ms=_sim_plugin_elapsed_ms(persist_started_perf),
        )

    _trading_log.info(
        "session=%s START_PREP pid=%s thread=%s workers=%d",
        session_id, os.getpid(), threading.current_thread().name, len(sessions_store),
    )

    if not session_id:
        raise HTTPException(status_code=400, detail="session_id missing")

    # Use the robust getter to handle cross-worker synchronization
    _trading_log.info(
        "session=%s START_SESSION_RETRIEVED status=%s",
        session_id, session_data.get('status') if session_data else None,
    )

    if not session_data:
        raise HTTPException(status_code=404, detail="Session not found or expired")

    if session_data.get("status") != "authenticated":
        if is_simulation_start:
            _sim_plugin_log(
                "start_trading ABORT not authenticated",
                session_id=session_id,
                status=session_data.get("status"),
                total_elapsed_ms=_sim_plugin_elapsed_ms(req_started_perf),
            )
        _trading_log.warning(
            "session=%s START_UNAUTHENTICATED status=%s", session_id, session_data.get('status')
        )
        raise HTTPException(
            status_code=401,
            detail=f"Session not authenticated. Current status: {session_data.get('status')}"
        )

    try:
        broker = session_data.get("broker")
        broker_session = session_data.get("broker_session")
        free_cash = session_data.get("free_cash") or 0

        if not broker or not broker_session:
            raise Exception("Broker connection not found")

        # Calculate total allocation
        total_allocated = sum(s.capital for s in config.symbols)

        # Candle handling (unchanged)
        if getattr(config, "candle", None):
            await run_in_threadpool(persist_session_metadata_sync, session_id, {"candle_interval": str(config.candle)})
            sessions_store[session_id]["candle_interval"] = str(config.candle)

        session_candle = session_data.get("candle_interval")

        class WebAlertManager:
            def __init__(self, session_id):
                self.session_id = session_id

            def notify(self, message):
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                _trading_log.info("session=%s ALERT message=%s", self.session_id, message)
                add_log(self.session_id, f"[ALERT] {message}")
                # Track market close exit
                if "2:30 PM" in message or "Auto Trader Stopped" in message:
                    if self.session_id in trading_status:
                        trading_status[self.session_id]["exit_initiated"] = True
                        trading_status[self.session_id]["exit_time"] = datetime.now().isoformat()

        alerts = WebAlertManager(session_id)

        # Build FULL symbols payload directly from config
        symbols_payload = [
            {
                "symbol": s.symbol,
                "capital": float(s.capital),
                "stop_loss": float(s.stop_loss)
            }
            for s in config.symbols
        ]

        # Inject into in-memory session_data
        session_data["strategy"] = config.strategy
        session_data["symbols"] = symbols_payload
        session_data["time_frame"] = config.time_frame
        session_data["candle"] = config.candle
        if config.configuration_id:
            session_data["configuration_id"] = config.configuration_id
        if config.leverage_multiplier is not None:
            session_data["leverage_multiplier"] = float(config.leverage_multiplier)

        _trading_log.info("session=%s SYMBOLS_PAYLOAD symbols=%s", session_id, [s['symbol'] for s in symbols_payload])        # Persist for UI/history (optional)
        persist_payload = {
            "strategy": config.strategy,
            "symbols": symbols_payload,
            "time_frame": config.time_frame,
            "candle": config.candle,
        }
        if config.configuration_id:
            persist_payload["configuration_id"] = config.configuration_id
        if config.leverage_multiplier is not None:
            persist_payload["leverage_multiplier"] = float(config.leverage_multiplier)
        persist_started_perf = time.perf_counter()
        await run_in_threadpool(persist_session_metadata_sync, session_id, persist_payload)
        if is_simulation_start:
            _sim_plugin_log(
                "start_trading phase=persist_session_metadata_sync (final)",
                session_id=session_id,
                configuration_id=config.configuration_id,
                strategy=config.strategy,
                elapsed_ms=_sim_plugin_elapsed_ms(persist_started_perf),
            )

        # DIRECTLY start trader using live session_data
        worker_started_perf = time.perf_counter()
        is_live_start = config.strategy != "B"
        if is_live_start:
            prep = await run_in_threadpool(SessionManager.prepare_for_live_start, session_id)
            if prep == LiveStartPrepResult.LIVE_ALREADY_RUNNING:
                _trading_log.info("session=%s LIVE_START_IDEMPOTENT worker_running", session_id)
                return {
                    "success": True,
                    "message": "Trading already active on plugin",
                    "session_id": session_id,
                    "total_allocated": total_allocated,
                    "free_cash": free_cash,
                    "symbols": [s.symbol for s in config.symbols],
                    "already_running": True,
                }
            if prep == LiveStartPrepResult.NOT_CLEARED:
                raise HTTPException(
                    status_code=503,
                    detail=(
                        "Previous simulation/trading worker did not shut down in time. "
                        "Wait a few seconds and retry live start."
                    ),
                )
            _trading_log.info(
                "session=%s LIVE_START_PREP_RESULT result=%s gunicorn_pid=%s",
                session_id, prep, os.getpid(),
            )

        def _spawn_worker():
            SessionManager.start_session(
                session_id=session_id,
                session_doc=session_data,
                trading_logs_collection=trading_logs_collection,
            )

        try:
            _spawn_worker()
        except RuntimeError as runtime_err:
            err_text = str(runtime_err)
            if "already running" in err_text.lower():
                worker_status = SessionManager.get_session_status(session_id)
                if is_live_start:
                    if worker_status and worker_status.get("is_alive"):
                        _trading_log.info("session=%s LIVE_START_IDEMPOTENT worker_alive", session_id)
                        return {
                            "success": True,
                            "message": "Trading already active on plugin",
                            "session_id": session_id,
                            "total_allocated": total_allocated,
                            "free_cash": free_cash,
                            "symbols": [s.symbol for s in config.symbols],
                            "already_running": True,
                        }
                    raise HTTPException(
                        status_code=503,
                        detail=(
                            "Could not start live trading — worker registry conflict. "
                            "Retry in a few seconds."
                        ),
                    )
                elif worker_status and worker_status.get("is_alive"):
                    if is_simulation_start:
                        _sim_plugin_log(
                            "start_trading idempotent already running",
                            session_id=session_id,
                            worker_elapsed_ms=_sim_plugin_elapsed_ms(worker_started_perf),
                            total_elapsed_ms=_sim_plugin_elapsed_ms(req_started_perf),
                        )
                    return {
                        "success": True,
                        "message": "Trading already active on plugin",
                        "session_id": session_id,
                        "total_allocated": total_allocated,
                        "free_cash": free_cash,
                        "symbols": [s.symbol for s in config.symbols],
                        "already_running": True,
                    }
                else:
                    await run_in_threadpool(SessionManager.ensure_worker_stopped, session_id)
                    _spawn_worker()
            else:
                _trading_log.error(
                    "session=%s START_RUNTIME_ERROR strategy=%s gunicorn_pid=%s error=%r",
                    session_id, config.strategy, os.getpid(), runtime_err,
                )
                raise

        if is_simulation_start:
            _sim_plugin_log(
                "start_trading phase=SessionManager.start_session",
                session_id=session_id,
                configuration_id=config.configuration_id,
                worker_elapsed_ms=_sim_plugin_elapsed_ms(worker_started_perf),
                total_elapsed_ms=_sim_plugin_elapsed_ms(req_started_perf),
            )

        response_payload = {
            "success": True,
            "message": "Trading started successfully",
            "session_id": session_id,
            "total_allocated": total_allocated,
            "free_cash": free_cash,
            "symbols": [s.symbol for s in config.symbols],
        }
        if config.leverage_multiplier is not None:
            response_payload["leverage_multiplier"] = float(config.leverage_multiplier)
        if is_simulation_start:
            _sim_plugin_log(
                "start_trading EXIT success",
                session_id=session_id,
                configuration_id=config.configuration_id,
                total_elapsed_ms=_sim_plugin_elapsed_ms(req_started_perf),
            )
        return response_payload

    except HTTPException:
        raise
    except Exception as e:
        _trading_log.exception(
            "session=%s START_FAILED strategy=%s gunicorn_pid=%s error=%r",
            session_id, getattr(config, 'strategy', '?'), os.getpid(), e,
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to start trading: {str(e)}"
        )


@router.post("/api/trading/start-simulation")
async def start_trading_simulation(config: TradingConfig, x_plugin_api_key: str = Header(None)):
    """
    Start paper simulation with the same payload as /api/trading/start.
    Always uses strategy B -> AutoTrader from auto_trader_exposure_expansion.py.
    """
    endpoint_started_perf = time.perf_counter()
    endpoint_started_ms = _sim_plugin_now_ms()
    incoming = _sim_plugin_payload_summary(config)
    _sim_plugin_log(
        "POST /api/trading/start-simulation ENTER",
        started_at_ms=endpoint_started_ms,
        incoming_payload=incoming,
        configuration_id_present=bool(config.configuration_id),
    )

    sim_config = config.model_copy(update={"strategy": "B"})
    _sim_plugin_log(
        "POST /api/trading/start-simulation normalized",
        session_id=sim_config.session_id,
        forced_strategy="B",
        configuration_id=sim_config.configuration_id,
        symbol_count=len(sim_config.symbols),
    )

    result = await start_trading(sim_config, x_plugin_api_key)

    if isinstance(result, dict) and result.get("success"):
        result = {
            **result,
            "message": "Paper simulation started successfully",
            "mode": "simulation",
            "strategy": "B",
            "trader_module": "auto_trader_exposure_expansion",
            "configuration_id": sim_config.configuration_id,
            "leverage_multiplier": sim_config.leverage_multiplier,
            "timing_ms": _sim_plugin_elapsed_ms(endpoint_started_perf),
        }
        _sim_plugin_log(
            "POST /api/trading/start-simulation EXIT success",
            session_id=sim_config.session_id,
            configuration_id=sim_config.configuration_id,
            total_elapsed_ms=_sim_plugin_elapsed_ms(endpoint_started_perf),
        )
    else:
        _sim_plugin_log(
            "POST /api/trading/start-simulation EXIT non-success",
            session_id=sim_config.session_id,
            result=result,
            total_elapsed_ms=_sim_plugin_elapsed_ms(endpoint_started_perf),
        )
    return result


@router.get("/api/trading/status/{session_id}")
async def get_trading_status(session_id: str):
    """Get current trading status for a session"""
    sync_exit_status_from_redis(session_id)
    session_exists = (
        session_id in sessions_store or
        session_id in trading_status or
        session_id in trading_logs
    )
    db_record = None
    if DB_CONNECTED:
        db_record = await fetch_session_from_db(session_id)
        if db_record:
            session_exists = True

    if not session_exists:
        raise HTTPException(status_code=404, detail="Session not found")

    status = trading_status.get(session_id)
    logs = trading_logs.get(session_id, [])

    if not status and db_record:
        status = {
            "status": db_record.get("trading_status") or db_record.get("status") or "unknown",
            "started_at": db_record.get("trading_started_at"),
            "symbols": db_record.get("symbols", []),
            "total_capital": db_record.get("total_capital", 0),
            "error": db_record.get("error"),
            "exit_initiated": db_record.get("exit_initiated", False),
            "exit_time": db_record.get("exit_time"),
        }

    if not logs and db_record:
        logs = await fetch_logs_from_db(session_id, limit=100)

    started_at = (status or {}).get("started_at") or (db_record and db_record.get("trading_started_at"))
    symbols = (status or {}).get("symbols") or (db_record and db_record.get("symbols", []))
    total_capital = (status or {}).get("total_capital") or (db_record and db_record.get("total_capital", 0))
    error = (status or {}).get("error") or (db_record and db_record.get("error"))
    status_name = (status or {}).get("status", "unknown")
    if db_record and (db_record.get("status") == "stopped" or db_record.get("trading_status") == "stopped"):
        status_name = "stopped"

    worker_status = SessionManager.get_session_status(session_id)
    worker_active = bool(worker_status and worker_status.get("is_alive"))

    exit_initiated = (status or {}).get("exit_initiated")
    if exit_initiated is None and db_record:
        exit_initiated = db_record.get("exit_initiated", False)
    exit_initiated = bool(exit_initiated)
    exit_time = (status or {}).get("exit_time") or (db_record and db_record.get("exit_time"))

    return {
        "success": True,
        "status": status_name,
        "worker_active": worker_active,
        "worker": worker_status,
        "started_at": started_at,
        "symbols": symbols,
        "total_capital": total_capital,
        "error": error,
        "exit_initiated": exit_initiated,
        "exit_time": exit_time,
        "logs": (logs or [])[-100:]
    }


@router.get("/api/trading/pyramid-pnl/{session_id}")
async def get_pyramid_pnl(session_id: str, x_plugin_api_key: str = Header(None)):
    if not DB_CONNECTED or pyramid_pnls_collection is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    doc = await run_in_threadpool(
        pyramid_pnls_collection.find_one,
        {"session_id": session_id},
        {"_id": 0},
    )
    if not doc:
        raise HTTPException(status_code=404, detail="Pyramid PnL snapshot not found")
    return {"success": True, **doc}


@router.get("/api/trading/exited-symbols/{session_id}")
async def get_exited_symbols(session_id: str, x_plugin_api_key: str = Header(None)):
    if not DB_CONNECTED or exited_symbols_collection is None:
        logger.warning("[EXITED-SYMBOLS-API] database unavailable session_id=%s", session_id)
        raise HTTPException(status_code=503, detail="Database unavailable")

    logger.info("[EXITED-SYMBOLS-API] fetch start session_id=%s", session_id)
    cursor = exited_symbols_collection.find(
        {"session_id": session_id},
        {"_id": 0},
    ).sort([
        ("exit_time_utc", 1),
        ("updated_at", 1),
        ("symbol", 1),
    ])
    symbols = await run_in_threadpool(list, cursor)
    logger.info(
        "[EXITED-SYMBOLS-API] fetch done session_id=%s count=%s symbols=%s",
        session_id,
        len(symbols),
        [row.get("symbol") for row in symbols],
    )
    return {
        "success": True,
        "session_id": session_id,
        "count": len(symbols),
        "symbols": symbols,
    }


@router.get("/api/trading/exit-status/{session_id}")
async def get_exit_status(session_id: str, x_plugin_api_key: str = Header(None)):
    redis_exit = sync_exit_status_from_redis(session_id)

    if session_id not in trading_status:
        # Prefer worker Redis signal even when in-memory trading_status is empty
        if redis_exit and redis_exit.get("exit_initiated"):
            return {
                "success": True,
                "session_id": session_id,
                "exit_initiated": True,
                "exit_time": redis_exit.get("exit_time"),
                "status": "completed_exit",
                "message": "All positions exited at 15:00 IST",
                "reason": redis_exit.get("reason"),
            }
        # Check DB if not in memory
        if DB_CONNECTED:
            db_record = await fetch_session_from_db(session_id)
            if db_record:
                exit_initiated = bool(db_record.get("exit_initiated", False))
                return {
                    "success": True,
                    "session_id": session_id,
                    "exit_initiated": exit_initiated,
                    "exit_time": db_record.get("exit_time"),
                    "status": db_record.get("trading_status", "unknown"),
                    "message": "All positions exited at 15:00 IST" if exit_initiated else None,
                }

        raise HTTPException(status_code=404, detail="Session not found")

    status = trading_status[session_id]
    exit_initiated = bool(status.get("exit_initiated", False))

    return {
        "success": True,
        "session_id": session_id,
        "exit_initiated": exit_initiated,
        "exit_time": status.get("exit_time"),
        "status": status.get("status"),
        "message": "All positions exited at 15:00 IST" if exit_initiated else None,
        "reason": status.get("exit_reason"),
    }


@router.get("/api/trading/logs/{session_id}")
async def get_trading_logs(session_id: str, x_plugin_api_key: str = Header(None)):
    """Get trading logs for a session (for real-time streaming)"""
    x_plugin_api_key: str = Header(None)
    session_exists = session_id in sessions_store
    db_record = None
    if not session_exists and DB_CONNECTED:
        db_record = await fetch_session_from_db(session_id)
        session_exists = db_record is not None

    if not session_exists:
        raise HTTPException(status_code=404, detail="Session not found")

    logs = trading_logs.get(session_id)
    if logs is None and DB_CONNECTED:
        logs = await fetch_logs_from_db(session_id, limit=100)
    logs = logs or []
    return {
        "success": True,
        "logs": logs,
        "count": len(logs)
    }


@router.get("/api/trading/{session_id}/final-tradebook")
def get_final_tradebook(
    session_id: str,
    x_plugin_api_key: str = Header(None)
):
    # 1) Try session-bound trader
    session_data = sessions_store.get(session_id)
    trader = session_data.get("trader") if session_data else None

    if trader:
        date_str = trader._now_market_time().strftime("%Y-%m-%d")
        filename = f"final_tradebook_{date_str}.csv"
        file_path = Path(trader.log_dir) / filename

        if file_path.exists():
            return FileResponse(
                path=file_path,
                filename=filename,
                media_type="text/csv",
            )

    # 2) FALLBACK: latest merged tradebook
    candidates = sorted(
        LOGS_DIR.glob("final_tradebook_*.csv"),
        reverse=True
    )

    if not candidates:
        raise HTTPException(
            status_code=404,
            detail="No final tradebook found (no active session & no backup file)"
        )

    file_path = candidates[0]

    return FileResponse(
        path=file_path,
        filename=file_path.name,
        media_type="text/csv",
    )


@router.post("/api/trading/stop/{session_id}")
async def stop_trading(session_id: str, x_plugin_api_key: str = Header(None)):
    await run_in_threadpool(SessionManager.stop_session, session_id)

    if session_id not in sessions_store:
        raise HTTPException(status_code=404, detail="Session not found")

    sessions_store[session_id]["status"] = "stopped"

    if session_id in trading_status:
        trading_status[session_id]["status"] = "stopped"

    add_log(session_id, "Trading stopped by user")

    await persist_session_state(session_id)

    return {
        "success": True,
        "message": "Trading session stop requested",
        "session_id": session_id
    }
