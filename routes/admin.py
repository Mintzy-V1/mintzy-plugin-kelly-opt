"""Admin routes: session management, saved configurations, trading-log exports."""
import csv
import io
import logging
import zipfile
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, Header, HTTPException, Query
from fastapi.responses import StreamingResponse
from starlette.concurrency import run_in_threadpool

from core.config import MONGO_CONFIG_DB_NAME
from core.logging import get_logger
from core.security import verify_plugin_key
from repositories.db import (
    DB_CONNECTED,
    SAVED_TRADING_CONFIGURATION_COLLECTION,
    _configuration_shape_summary,
    _parse_configuration_object_id,
    _saved_trading_configuration_collection,
    _serialize_saved_configuration_doc,
    _trading_logs_mongo_query,
    sessions_collection,
    trading_logs_collection,
)
from session_manager import SessionManager
from state import sessions_store, trading_logs, trading_status

logger = logging.getLogger(__name__)
_admin_log = get_logger("ADMIN")

router = APIRouter()


@router.delete("/api/admin/sessions/{session_id}")
async def delete_session_by_id(
    session_id: str,
    x_plugin_api_key: str = Header(None)
):
    if not DB_CONNECTED:
        raise HTTPException(status_code=500, detail="DB not connected")

    result = sessions_collection.delete_one({"session_id": session_id})

    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Session not found")

    # Best-effort memory cleanup
    sessions_store.pop(session_id, None)
    trading_status.pop(session_id, None)
    trading_logs.pop(session_id, None)

    return {
        "success": True,
        "message": "Session deleted successfully",
        "session_id": session_id
    }


@router.get("/api/admin/saved-configurations/{configuration_id}")
async def admin_get_saved_configuration(
    configuration_id: str,
    x_plugin_api_key: str = Header(None),
):
    """Fetch one SavedTradingConfiguration from MONGO_CONFIG_DB_NAME (default: test)."""
    await verify_plugin_key(x_plugin_api_key)

    coll = _saved_trading_configuration_collection()
    oid = _parse_configuration_object_id(configuration_id)
    doc = coll.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="SavedTradingConfiguration not found")

    configuration = doc.get("configuration")
    return {
        "success": True,
        "database": MONGO_CONFIG_DB_NAME,
        "collection": SAVED_TRADING_CONFIGURATION_COLLECTION,
        "configuration_id": configuration_id,
        "document": _serialize_saved_configuration_doc(doc),
        "configuration_summary": _configuration_shape_summary(configuration),
    }


@router.put("/api/admin/saved-configurations/{configuration_id}")
async def admin_update_saved_configuration(
    configuration_id: str,
    body: Dict[str, Any] = Body(
        ...,
        description=(
            "Full editable record payload. Send name, description, configuration exactly as "
            "stored in Mongo — including every nested field (base_multiplier, role, kelly_config, "
            "alphas.updated_at, etc.). Nothing is stripped or normalized. "
            "Only _id and user_id on the existing record are preserved; all sent top-level "
            "fields replace the stored values."
        ),
    ),
    x_plugin_api_key: str = Header(None),
):
    """
    Replace SavedTradingConfiguration content from a raw JSON body.

    Immutable on the existing record: _id, user_id, created_at.
    Pass-through: configuration (and all nested keys) is stored exactly as sent.
    """
    await verify_plugin_key(x_plugin_api_key)

    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Body must be a JSON object")

    coll = _saved_trading_configuration_collection()
    oid = _parse_configuration_object_id(configuration_id)

    existing = coll.find_one({"_id": oid})
    if not existing:
        raise HTTPException(status_code=404, detail="SavedTradingConfiguration not found")

    from repositories.db import SAVED_CONFIG_IMMUTABLE_FIELDS
    incoming = {
        key: value
        for key, value in body.items()
        if key not in SAVED_CONFIG_IMMUTABLE_FIELDS
    }
    if not incoming:
        raise HTTPException(
            status_code=400,
            detail=(
                "Send at least one editable field (e.g. name, description, configuration). "
                f"Ignored keys: {sorted(SAVED_CONFIG_IMMUTABLE_FIELDS)}"
            ),
        )

    if "configuration" in incoming and not isinstance(incoming["configuration"], dict):
        raise HTTPException(status_code=400, detail="configuration must be a JSON object")

    before_summary = _configuration_shape_summary(existing.get("configuration"))

    # Merge: keep immutable fields from DB, overwrite everything else from body exactly.
    replacement = dict(existing)
    replacement.update(incoming)
    replacement["_id"] = existing["_id"]
    replacement["user_id"] = existing["user_id"]
    if existing.get("created_at") is not None:
        replacement["created_at"] = existing["created_at"]
    replacement["updated_at"] = datetime.utcnow()

    logger.info(
        "[ADMIN-SAVED-CONFIG] full replace configuration_id=%s db=%s keys=%s before=%s after=%s",
        configuration_id,
        MONGO_CONFIG_DB_NAME,
        sorted(incoming.keys()),
        before_summary,
        _configuration_shape_summary(replacement.get("configuration")),
    )

    result = coll.replace_one({"_id": oid}, replacement)
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="SavedTradingConfiguration not found")

    updated = coll.find_one({"_id": oid})
    return {
        "success": True,
        "message": "SavedTradingConfiguration replaced (exact payload stored)",
        "database": MONGO_CONFIG_DB_NAME,
        "collection": SAVED_TRADING_CONFIGURATION_COLLECTION,
        "configuration_id": configuration_id,
        "replaced_fields": sorted(incoming.keys()),
        "before": before_summary,
        "after": _configuration_shape_summary(updated.get("configuration") if updated else None),
        "document": _serialize_saved_configuration_doc(updated),
    }


@router.post("/api/admin/sessions/{session_id}/stop")
async def mark_session_stopped(
    session_id: str,
    x_plugin_api_key: str = Header(None)
):
    _admin_log.info("session=%s ADMIN_STOP_REQUESTED db_connected=%s", session_id, DB_CONNECTED)

    logger.info("[ADMIN STOP] Request received")
    logger.info("[ADMIN STOP] session_id=%s", session_id)
    logger.info("[ADMIN STOP] x_plugin_api_key_present=%s", bool(x_plugin_api_key))
    logger.info("[ADMIN STOP] DB_CONNECTED=%s", DB_CONNECTED)
    await run_in_threadpool(SessionManager.stop_session, session_id)

    # Auth check
    try:
        # verify_plugin_key(x_plugin_api_key)
        _admin_log.info("session=%s ADMIN_STOP_PLUGIN_KEY_VERIFIED", session_id)
    except Exception as e:
        _admin_log.error("session=%s ADMIN_STOP_PLUGIN_KEY_FAILED error=%s", session_id, e)
        logger.error("[ADMIN STOP] Plugin key verification failed")
        raise

    if not DB_CONNECTED:
        _admin_log.warning("session=%s ADMIN_STOP_DB_NOT_CONNECTED", session_id)
        raise HTTPException(status_code=500, detail="DB not connected")

    # Check session existence FIRST
    existing = sessions_collection.find_one({"session_id": session_id})
    _admin_log.info("session=%s ADMIN_STOP_SESSION_FOUND found=%s", session_id, bool(existing))

    if not existing:
        _admin_log.warning("session=%s ADMIN_STOP_SESSION_NOT_FOUND", session_id)
        raise HTTPException(status_code=404, detail="Session not found in DB")

    # Update status
    result = sessions_collection.update_one(
        {"session_id": session_id},
        {
            "$set": {
                "status": "stopped",
                "stopped_at": datetime.utcnow(),
                "last_updated": datetime.utcnow()
            }
        }
    )

    logger.info(
        "[ADMIN STOP] Update result matched=%s modified=%s",
        result.matched_count,
        result.modified_count
    )

    return {
        "success": True,
        "session_id": session_id,
        "new_status": "stopped"
    }


@router.get("/api/admin/trading-logs")
async def fetch_all_trading_logs(
    limit: int = 500,
    x_plugin_api_key: str = Header(None)
):
    if not DB_CONNECTED:
        raise HTTPException(status_code=500, detail="DB not connected")

    limit = max(1, min(limit, 2000))

    cursor = (
        trading_logs_collection
        .find({}, {"_id": 0})
        .sort("timestamp", -1)
        .limit(limit)
    )

    logs = []
    for doc in cursor:
        if isinstance(doc.get("timestamp"), datetime):
            doc["timestamp"] = doc["timestamp"].isoformat()
        logs.append(doc)

    return {
        "success": True,
        "count": len(logs),
        "logs": logs
    }


@router.get("/api/admin/trading-logs/user/{user_id}")
async def fetch_trading_logs_by_user(
    user_id: str,
    limit: int = 1000,
    x_plugin_api_key: str = Header(None)
):
    if not DB_CONNECTED:
        raise HTTPException(status_code=500, detail="DB not connected")

    limit = max(1, min(limit, 3000))

    # 1) Get all session_ids for this user
    session_ids = sessions_collection.distinct(
        "session_id",
        {"user_id": user_id}
    )

    if not session_ids:
        return {
            "success": True,
            "user_id": user_id,
            "count": 0,
            "logs": []
        }

    # 2) Fetch logs for those sessions
    cursor = (
        trading_logs_collection
        .find(
            {"session_id": {"$in": session_ids}},
            {"_id": 0}
        )
        .sort("timestamp", -1)
        .limit(limit)
    )

    logs = []
    for doc in cursor:
        if isinstance(doc.get("timestamp"), datetime):
            doc["timestamp"] = doc["timestamp"].isoformat()
        logs.append(doc)

    return {
        "success": True,
        "user_id": user_id,
        "sessions": session_ids,
        "count": len(logs),
        "logs": logs
    }


@router.get("/api/admin/trading-logs/user/{user_id}/download")
async def download_user_trading_logs_grouped(
    user_id: str,
    simulation_logs: Optional[bool] = Query(
        default=None,
        description="Filter by simulation (true) vs live (false). Omit for all logs.",
    ),
    x_plugin_api_key: str = Header(None)
):
    if not DB_CONNECTED:
        raise HTTPException(status_code=500, detail="DB not connected")

    # 1) Fetch session_ids for user
    session_ids = sessions_collection.distinct(
        "session_id",
        {"user_id": user_id}
    )

    if not session_ids:
        raise HTTPException(status_code=404, detail="No sessions found for user")

    # 2) Fetch all logs for those sessions
    cursor = trading_logs_collection.find(
        _trading_logs_mongo_query(session_ids=session_ids, simulation_logs=simulation_logs),
        {"_id": 0}
    ).sort([("session_id", 1), ("timestamp", 1)])

    # 3) Group logs by session_id
    grouped_logs = defaultdict(list)
    for doc in cursor:
        # Normalize timestamp
        if isinstance(doc.get("timestamp"), datetime):
            doc["timestamp"] = doc["timestamp"].strftime("%Y-%m-%d %H:%M:%S")

        grouped_logs[doc["session_id"]].append(doc)

    if not grouped_logs:
        raise HTTPException(status_code=404, detail="No trading logs found")

    # 4) Create ZIP in memory
    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
        for session_id, logs in grouped_logs.items():
            csv_buffer = io.StringIO()

            # Use keys of first log as CSV headers
            fieldnames = logs[0].keys()
            writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(logs)

            # Add CSV to ZIP
            zipf.writestr(
                f"{session_id}.csv",
                csv_buffer.getvalue()
            )

    zip_buffer.seek(0)

    # 5) Stream ZIP to client
    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={
            "Content-Disposition": f"attachment; filename=trading_logs_user_{user_id}.zip"
        }
    )
