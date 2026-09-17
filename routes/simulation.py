"""Simulation stop routes (async + legacy sync) and their shared helpers."""
import time
from typing import Any, Dict, Optional

from fastapi import APIRouter, Header, HTTPException, Query
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

from models import _sim_plugin_elapsed_ms, _sim_plugin_log, _sim_plugin_now_ms
from repositories.db import (
    DB_CONNECTED,
    add_log,
    fetch_session_from_db,
    persist_session_metadata_sync,
)
from services.session_service import get_or_restore_session
from session_manager import SessionManager
from state import sessions_store, trading_logs, trading_status

router = APIRouter()


async def _lookup_stop_simulation_session(session_id: str):
    session_exists = (
        session_id in sessions_store
        or session_id in trading_status
        or session_id in trading_logs
    )
    db_record = None
    if not session_exists and DB_CONNECTED:
        db_record = await fetch_session_from_db(session_id)
        session_exists = db_record is not None
    return session_exists, db_record


async def _finalize_stop_simulation_response(
    session_id: str,
    pyramid_handoff: Dict[str, Any],
    db_record: Optional[Dict[str, Any]],
    endpoint_started_perf: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Apply session store / Mongo updates after simulation worker stop + pyramid.
    Returns the same payload shape as the legacy synchronous stop-simulation endpoint.
    """
    live_allowed = bool(pyramid_handoff.get("live_allowed", False))
    post_restore_configuration_id = (
        sessions_store.get(session_id, {}).get("configuration_id")
        or (db_record or {}).get("configuration_id")
    )

    restore_started_perf = time.perf_counter()
    restored = await get_or_restore_session(session_id)

    timing_ms = (
        _sim_plugin_elapsed_ms(endpoint_started_perf)
        if endpoint_started_perf is not None
        else None
    )

    if not live_allowed:
        if session_id in sessions_store:
            sessions_store[session_id]["status"] = "stopped"
        elif restored:
            sessions_store[session_id] = {**restored, "status": "stopped"}
        elif session_id not in sessions_store:
            sessions_store[session_id] = {
                "session_id": session_id,
                "status": "stopped",
                "configuration_id": post_restore_configuration_id,
            }

        if session_id in trading_status:
            trading_status[session_id]["status"] = "stopped"
        else:
            trading_status[session_id] = {"status": "stopped"}

        if post_restore_configuration_id and session_id in sessions_store:
            sessions_store[session_id]["configuration_id"] = post_restore_configuration_id

        try:
            persist_payload = {
                "status": "stopped",
                "trading_status": "stopped",
                "stopped_reason": pyramid_handoff.get("reason") or "no_profitable_symbols",
            }
            if post_restore_configuration_id:
                persist_payload["configuration_id"] = post_restore_configuration_id
            await run_in_threadpool(persist_session_metadata_sync, session_id, persist_payload)
        except Exception as persist_err:
            _sim_plugin_log(
                "stop-simulation persist warning",
                session_id=session_id,
                error=str(persist_err),
            )

        SessionManager.clear_pyramid_handoff_result(session_id)
        add_log(
            session_id,
            "Paper simulation stopped — no profitable symbols; session marked stopped",
        )

        response_payload = {
            "success": True,
            "ready": True,
            "live_allowed": False,
            "pyramid": pyramid_handoff,
            "message": (
                "Simulation stopped. No profitable symbols — session closed. "
                "Create a new session for live trading."
            ),
            "session_id": session_id,
            "status": "stopped",
            "trading_status": "stopped",
            "configuration_id": post_restore_configuration_id,
        }
        if timing_ms is not None:
            response_payload["timing_ms"] = timing_ms
        return response_payload

    if session_id in trading_status:
        trading_status[session_id]["status"] = "simulation_stopped"

    if session_id in sessions_store:
        sessions_store[session_id]["status"] = "authenticated"
    elif restored:
        sessions_store[session_id] = {**restored, "status": "authenticated"}

    if post_restore_configuration_id and session_id in sessions_store:
        sessions_store[session_id]["configuration_id"] = post_restore_configuration_id

    try:
        persist_payload = {
            "status": "authenticated",
            "trading_status": "simulation_stopped",
        }
        if post_restore_configuration_id:
            persist_payload["configuration_id"] = post_restore_configuration_id
        await run_in_threadpool(persist_session_metadata_sync, session_id, persist_payload)
    except Exception as persist_err:
        _sim_plugin_log(
            "stop-simulation persist warning",
            session_id=session_id,
            error=str(persist_err),
        )

    SessionManager.clear_pyramid_handoff_result(session_id)
    add_log(session_id, "Paper simulation stopped (session remains authenticated)")

    response_payload = {
        "success": True,
        "ready": True,
        "live_allowed": live_allowed,
        "pyramid": pyramid_handoff,
        "message": "Simulation stopped; session remains authenticated",
        "session_id": session_id,
        "status": "authenticated",
        "trading_status": "simulation_stopped",
        "configuration_id": post_restore_configuration_id,
    }
    if timing_ms is not None:
        response_payload["timing_ms"] = timing_ms
    _ = restore_started_perf
    return response_payload


@router.post("/api/trading/stop-simulation/{session_id}")
async def stop_trading_simulation(
    session_id: str,
    wait: bool = Query(False, description="Block until worker stop completes (legacy sync mode)"),
    x_plugin_api_key: str = Header(None),
):
    """
    Stop the paper simulation worker.
    Default: async accept (202) — poll GET .../status for completion + pyramid handoff.
    ?wait=true: legacy blocking behaviour (may exceed gunicorn worker timeout).
    """
    endpoint_started_perf = time.perf_counter()
    endpoint_started_ms = _sim_plugin_now_ms()
    _sim_plugin_log(
        "POST /api/trading/stop-simulation ENTER",
        session_id=session_id,
        started_at_ms=endpoint_started_ms,
        wait=wait,
        body="(none — session_id is path param only)",
    )

    lookup_started_perf = time.perf_counter()
    session_exists, db_record = await _lookup_stop_simulation_session(session_id)

    _sim_plugin_log(
        "stop-simulation phase=session_lookup",
        session_id=session_id,
        session_exists=session_exists,
        in_memory_store=session_id in sessions_store,
        in_trading_status=session_id in trading_status,
        db_record_found=bool(db_record),
        stored_configuration_id=(
            sessions_store.get(session_id, {}).get("configuration_id")
            or (db_record or {}).get("configuration_id")
        ),
        elapsed_ms=_sim_plugin_elapsed_ms(lookup_started_perf),
    )

    if not session_exists:
        _sim_plugin_log(
            "stop-simulation ABORT session not found",
            session_id=session_id,
            total_elapsed_ms=_sim_plugin_elapsed_ms(endpoint_started_perf),
        )
        raise HTTPException(status_code=404, detail="Session not found")

    stop_worker_started_perf = time.perf_counter()

    if wait:
        stopped = await run_in_threadpool(SessionManager.stop_simulation_session, session_id)
        _sim_plugin_log(
            "stop-simulation phase=SessionManager.stop_simulation_session (sync wait=true)",
            session_id=session_id,
            stopped=stopped,
            elapsed_ms=_sim_plugin_elapsed_ms(stop_worker_started_perf),
        )
        if not stopped:
            raise HTTPException(
                status_code=503,
                detail="Failed to stop paper simulation (simulation-stop flag or worker shutdown failed)",
            )
        pyramid_handoff = SessionManager.read_pyramid_handoff_result(session_id) or {}
        response_payload = await _finalize_stop_simulation_response(
            session_id,
            pyramid_handoff,
            db_record,
            endpoint_started_perf,
        )
        _sim_plugin_log(
            "POST /api/trading/stop-simulation EXIT sync",
            session_id=session_id,
            live_allowed=response_payload.get("live_allowed"),
            total_elapsed_ms=_sim_plugin_elapsed_ms(endpoint_started_perf),
        )
        return response_payload

    stopped = await run_in_threadpool(SessionManager.stop_simulation_session_async, session_id)
    _sim_plugin_log(
        "stop-simulation phase=SessionManager.stop_simulation_session_async",
        session_id=session_id,
        stopped=stopped,
        elapsed_ms=_sim_plugin_elapsed_ms(stop_worker_started_perf),
    )

    if not stopped:
        _sim_plugin_log(
            "stop-simulation ABORT async worker signal failed",
            session_id=session_id,
            total_elapsed_ms=_sim_plugin_elapsed_ms(endpoint_started_perf),
        )
        raise HTTPException(
            status_code=503,
            detail="Failed to queue paper simulation stop (simulation-stop flag or worker signal failed)",
        )

    accepted_payload = {
        "success": True,
        "accepted": True,
        "ready": False,
        "status": "stopping",
        "session_id": session_id,
        "message": "Simulation stop accepted; poll status endpoint for pyramid handoff",
        "poll_url": f"/api/trading/stop-simulation/{session_id}/status",
        "timing_ms": _sim_plugin_elapsed_ms(endpoint_started_perf),
    }
    _sim_plugin_log(
        "POST /api/trading/stop-simulation EXIT async 202",
        session_id=session_id,
        total_elapsed_ms=_sim_plugin_elapsed_ms(endpoint_started_perf),
    )
    return JSONResponse(status_code=202, content=accepted_payload)


@router.get("/api/trading/stop-simulation/{session_id}/status")
async def get_stop_simulation_status(session_id: str, x_plugin_api_key: str = Header(None)):
    """
    Poll async simulation stop progress. When status=completed, returns the same payload
    as a successful synchronous stop-simulation (including authenticated session for live handoff).
    """
    job = SessionManager.read_stop_job(session_id)
    if not job:
        session_exists, _ = await _lookup_stop_simulation_session(session_id)
        if not session_exists:
            raise HTTPException(status_code=404, detail="Session not found")
        return {
            "success": True,
            "ready": False,
            "status": "not_started",
            "session_id": session_id,
            "message": "No stop job found for this session",
        }

    job_status = job.get("status")

    if job_status == "stopping":
        return {
            "success": True,
            "accepted": True,
            "ready": False,
            "status": "stopping",
            "phase": job.get("phase"),
            "session_id": session_id,
            "started_at": job.get("started_at"),
            "poll_url": f"/api/trading/stop-simulation/{session_id}/status",
        }

    if job_status == "failed":
        return {
            "success": False,
            "ready": True,
            "status": "failed",
            "session_id": session_id,
            "error": job.get("error") or "Simulation stop failed",
        }

    if job_status == "completed":
        cached = job.get("response")
        if job.get("finalized") and isinstance(cached, dict):
            return cached

        _, db_record = await _lookup_stop_simulation_session(session_id)
        pyramid_handoff = (
            job.get("pyramid")
            or SessionManager.read_pyramid_handoff_result(session_id)
            or {}
        )
        response_payload = await _finalize_stop_simulation_response(
            session_id,
            pyramid_handoff,
            db_record,
        )
        SessionManager.update_stop_job(
            session_id,
            finalized=True,
            response=response_payload,
        )
        return response_payload

    return {
        "success": True,
        "ready": False,
        "status": job_status or "unknown",
        "session_id": session_id,
    }
