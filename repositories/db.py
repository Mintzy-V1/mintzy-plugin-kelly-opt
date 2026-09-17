"""MongoDB persistence for plugin sessions, logs, and trading runtime state."""
import io
import logging
import sys
from contextlib import redirect_stdout
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import HTTPException
from pymongo import MongoClient
from starlette.concurrency import run_in_threadpool

from core.config import (
    MONGO_CONFIG_DB_NAME,
    MONGO_DB_NAME,
    MONGO_URI,
    _decrypt_val,
    _encrypt_val,
)
from core.logging import get_logger
from repositories.redis import drain_rms_exited_symbols, sync_exit_status_from_redis
from state import _state_lock, sessions_store, trading_logs, trading_status

logger = logging.getLogger(__name__)
_db_log = get_logger("DATABASE")

DB_CONNECTED = False
mongo_client = None
mongo_db = None
sessions_collection = None
logs_collection = None
trading_logs_collection = None
pyramid_pnls_collection = None
exited_symbols_collection = None

try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    mongo_db = mongo_client[MONGO_DB_NAME]
    sessions_collection = mongo_db["plugin_sessions"]
    logs_collection = mongo_db["plugin_logs"]
    trading_logs_collection = mongo_db["trading_logs"]
    pyramid_pnls_collection = mongo_client[MONGO_CONFIG_DB_NAME]["pyramid_pnls"]
    exited_symbols_collection = mongo_client[MONGO_CONFIG_DB_NAME]["exited_symbols"]
    try:
        exited_symbols_collection.create_index(
            [("session_id", 1), ("symbol", 1)],
            unique=True,
            name="uniq_session_symbol_exit",
        )
        exited_symbols_collection.create_index(
            [("session_id", 1), ("status", 1)],
            name="idx_session_exit_status",
        )
    except Exception as index_exc:
        logger.warning("exited_symbols index ensure failed (%s)", index_exc)

    # Force server selection to verify connectivity
    mongo_client.admin.command('ping')
    DB_CONNECTED = True
    logger.info("Connected to MongoDB for plugin persistence")
    if MONGO_CONFIG_DB_NAME != MONGO_DB_NAME:
        logger.info(
            "SavedTradingConfiguration uses DB %s (plugin sessions/logs: %s)",
            MONGO_CONFIG_DB_NAME,
            MONGO_DB_NAME,
        )
except Exception as exc:
    logger.warning("MongoDB persistence unavailable (%s)", exc)
    pyramid_pnls_collection = None
    exited_symbols_collection = None


def _trading_logs_mongo_query(
    *,
    session_id: Optional[str] = None,
    session_ids: Optional[List[str]] = None,
    simulation_logs: Optional[bool] = None,
) -> dict:
    """Build Mongo filter for trading_logs. Legacy rows without simulation_logs count as live."""
    if session_ids is not None:
        query: dict = {"session_id": {"$in": session_ids}}
    elif session_id:
        query = {"session_id": session_id}
    else:
        raise ValueError("session_id or session_ids is required")

    if simulation_logs is True:
        query["simulation_logs"] = True
    elif simulation_logs is False:
        query["simulation_logs"] = {"$ne": True}
    return query


SAVED_TRADING_CONFIGURATION_COLLECTION = "savedtradingconfigurations"
SAVED_CONFIG_IMMUTABLE_FIELDS = frozenset(
    {"_id", "user_id", "created_at", "__v", "updated_at"}
)


def _saved_trading_configuration_collection():
    if not DB_CONNECTED or mongo_client is None:
        raise HTTPException(status_code=503, detail="MongoDB not connected")
    return mongo_client[MONGO_CONFIG_DB_NAME][SAVED_TRADING_CONFIGURATION_COLLECTION]


def _parse_configuration_object_id(configuration_id: str):
    try:
        from bson import ObjectId
    except ImportError as exc:
        raise HTTPException(status_code=500, detail=f"bson unavailable: {exc}") from exc

    if not ObjectId.is_valid(configuration_id):
        raise HTTPException(status_code=400, detail=f"Invalid configuration id: {configuration_id}")
    return ObjectId(configuration_id)


def _serialize_saved_configuration_doc(doc: Optional[dict]) -> Optional[dict]:
    if not doc:
        return doc
    serialized = dict(doc)
    if serialized.get("_id") is not None:
        serialized["_id"] = str(serialized["_id"])
    if serialized.get("user_id") is not None:
        serialized["user_id"] = str(serialized["user_id"])
    for key in ("created_at", "updated_at"):
        value = serialized.get(key)
        if isinstance(value, datetime):
            serialized[key] = value.isoformat()
    return serialized


def _configuration_shape_summary(configuration: Optional[dict]) -> dict:
    if not isinstance(configuration, dict):
        return {"configuration_type": type(configuration).__name__}

    symbols = configuration.get("symbols")
    alphas = configuration.get("alphas")
    alpha_symbols = None
    if isinstance(alphas, dict):
        alpha_symbols = alphas.get("symbols")
    elif isinstance(alphas, list):
        alpha_symbols = alphas

    return {
        "configuration_keys": list(configuration.keys()),
        "symbols_count": len(symbols) if isinstance(symbols, list) else None,
        "alphas_type": type(alphas).__name__ if alphas is not None else None,
        "alphas_symbols_count": len(alpha_symbols) if isinstance(alpha_symbols, list) else None,
        "strategy": configuration.get("strategy"),
        "candle": configuration.get("candle"),
    }


def _ensure_session_doc(session_id: str, payload: Dict[str, Any]) -> None:
    if not DB_CONNECTED or not payload:
        return
    try:
        now = datetime.utcnow()
        update_payload = {
            "$set": {**payload, "last_updated": now},
            "$setOnInsert": {"created_at": now}
        }
        sessions_collection.update_one({"session_id": session_id}, update_payload, upsert=True)
    except Exception as exc:
        _db_log.warning("session=%s PERSIST_SESSION_FAILED error=%s", session_id, exc)


def persist_session_metadata_sync(session_id: str, payload: Dict[str, Any]) -> None:
    _ensure_session_doc(session_id, payload)


async def persist_session_metadata(session_id: str, payload: Dict[str, Any]) -> None:
    if not payload:
        return
    await run_in_threadpool(persist_session_metadata_sync, session_id, payload)


def persist_log_sync(session_id: str, message: str, level: str = "info") -> None:
    if not DB_CONNECTED:
        return
    try:
        logs_collection.insert_one({
            "session_id": session_id,
            "message": message,
            "level": level,
            "timestamp": datetime.utcnow()
        })
    except Exception as exc:
        logger.warning("Failed to persist log entry (%s): %s", session_id, exc)


def _fetch_session_sync(session_id: str) -> Optional[Dict[str, Any]]:
    if not DB_CONNECTED:
        return None
    doc = sessions_collection.find_one({"session_id": session_id}, projection={"_id": False})
    if doc:
        for datetime_field in ("created_at", "authenticated_at", "trading_started_at", "last_activity", "last_updated"):
            if isinstance(doc.get(datetime_field), datetime):
                doc[datetime_field] = doc[datetime_field].isoformat()
    return doc


async def fetch_session_from_db(session_id: str) -> Optional[Dict[str, Any]]:
    """Fetch persisted session doc and decrypt credentials/tokens if stored."""
    if not DB_CONNECTED:
        return None
    doc = await run_in_threadpool(sessions_collection.find_one, {"session_id": session_id})
    if not doc:
        return None

    restored = {
        "session_id": doc.get("session_id"),
        "status": doc.get("status"),

        "free_cash": doc.get("free_cash"),
        "created_at": doc.get("created_at"),
        "authenticated_at": doc.get("authenticated_at"),
        "strategy": doc.get("strategy", "A"),
        "configuration_id": doc.get("configuration_id"),
    }

    api_key = _decrypt_val(doc.get("api_key"))
    client_code = _decrypt_val(doc.get("client_code"))
    password = _decrypt_val(doc.get("password"))
    if api_key:
        restored["api_key"] = api_key
    if client_code:
        restored["client_code"] = client_code
    if password:
        restored["password"] = password

    bs = doc.get("broker_session")
    if isinstance(bs, dict):
        restored_bs = {
            "user": bs.get("user"),
            "token": _decrypt_val(bs.get("token")),
            "refresh_token": _decrypt_val(bs.get("refresh_token")),
            "feed_token": _decrypt_val(bs.get("feed_token"))
        }
        restored["broker_session"] = {k: v for k, v in restored_bs.items() if v is not None}

    return restored


def fetch_logs_sync(session_id: str, limit: int = 100) -> List[Dict[str, Any]]:
    if not DB_CONNECTED:
        return []
    try:
        cursor = logs_collection.find({"session_id": session_id}).sort("timestamp", -1).limit(limit)
        entries: List[Dict[str, Any]] = []
        for doc in cursor:
            timestamp = doc.get("timestamp")
            entries.append({
                "message": doc.get("message", ""),
                "level": doc.get("level"),
                "timestamp": timestamp.isoformat() if isinstance(timestamp, datetime) else timestamp
            })
        return entries
    except Exception as exc:
        logger.warning("Failed to fetch logs from DB (%s): %s", session_id, exc)
        return []


async def fetch_logs_from_db(session_id: str, limit: int = 100) -> List[Dict[str, Any]]:
    return await run_in_threadpool(fetch_logs_sync, session_id, limit)


def _session_metadata_payload(session_id: str) -> Dict[str, Any]:
    # Per-ticker RMS exits arrive via Redis from the trader worker process;
    # drain before snapshotting so the returned `symbols` list is current.
    drain_rms_exited_symbols(session_id)
    sync_exit_status_from_redis(session_id)
    session_data = sessions_store.get(session_id, {})
    status_data = trading_status.get(session_id, {})
    if not session_data and not status_data:
        return {}

    bs = session_data.get("broker_session") or {}
    broker_session_payload = None
    if isinstance(bs, dict):
        token_enc = _encrypt_val(bs.get("token") or bs.get("jwtToken") or bs.get("access_token"))
        if token_enc:
            broker_session_payload = {
                "user": bs.get("user"),
                "token": token_enc,
                "refresh_token": _encrypt_val(bs.get("refresh_token") or bs.get("refreshToken")),
                "feed_token": _encrypt_val(bs.get("feed_token") or bs.get("feedToken"))
            }

    ram_status = session_data.get("status")
    payload: Dict[str, Any] = {
        "session_id": session_id,
        "user_id": session_data.get("user_id"),
        "status": ram_status,
        "free_cash": session_data.get("free_cash"),
        "api_key": _encrypt_val(session_data.get("api_key")),
        "client_code": _encrypt_val(session_data.get("client_code")),
        "password": _encrypt_val(session_data.get("password")),
        "authenticated_at": session_data.get("authenticated_at"),
        "trading_status": status_data.get("status"),
        "trading_started_at": status_data.get("started_at"),
        "symbols": status_data.get("symbols"),
        "total_capital": status_data.get("total_capital"),
        "error": status_data.get("error"),
        "exit_initiated": status_data.get("exit_initiated"),
        "exit_time": status_data.get("exit_time"),
        "last_activity": datetime.utcnow()

    }
    if broker_session_payload:
        payload["broker_session"] = broker_session_payload

    # Multi-worker safety: never downgrade Mongo auth from stale in-process RAM.
    if DB_CONNECTED and ram_status != "authenticated":
        try:
            db_doc = sessions_collection.find_one({"session_id": session_id})
        except Exception:
            db_doc = None
        if db_doc:
            db_bs = db_doc.get("broker_session") if isinstance(db_doc.get("broker_session"), dict) else {}
            db_has_tokens = bool(db_bs.get("token"))
            db_was_authenticated = (
                db_doc.get("status") == "authenticated"
                or (db_doc.get("authenticated_at") and db_has_tokens)
            )
            if db_was_authenticated and db_has_tokens:
                payload["status"] = "authenticated"
                payload["broker_session"] = db_bs
                if db_doc.get("authenticated_at") is not None:
                    payload["authenticated_at"] = db_doc.get("authenticated_at")
                if db_doc.get("free_cash") is not None:
                    payload["free_cash"] = db_doc.get("free_cash")
            elif not broker_session_payload:
                payload.pop("broker_session", None)
                if db_doc.get("authenticated_at") and ram_status == "credentials_received":
                    payload.pop("status", None)

    return {k: v for k, v in payload.items() if v is not None or k == "session_id"}


def _trading_runtime_payload(session_id: str) -> Dict[str, Any]:
    """Persist only trading runtime fields — never session auth snapshots."""
    drain_rms_exited_symbols(session_id)
    sync_exit_status_from_redis(session_id)
    status_data = trading_status.get(session_id, {})
    session_data = sessions_store.get(session_id, {})
    if not status_data and not session_data:
        return {}
    payload: Dict[str, Any] = {
        "trading_status": status_data.get("status"),
        "trading_started_at": status_data.get("started_at"),
        "symbols": status_data.get("symbols") or session_data.get("symbols"),
        "total_capital": status_data.get("total_capital"),
        "error": status_data.get("error"),
        "exit_initiated": status_data.get("exit_initiated"),
        "exit_time": status_data.get("exit_time"),
        "last_activity": datetime.utcnow(),
    }
    return {k: v for k, v in payload.items() if v is not None}


def persist_trading_runtime_sync(session_id: str) -> None:
    payload = _trading_runtime_payload(session_id)
    if payload:
        persist_session_metadata_sync(session_id, payload)


def persist_session_state_sync(session_id: str) -> None:
    payload = _session_metadata_payload(session_id)
    if payload:
        persist_session_metadata_sync(session_id, payload)


async def persist_session_state(session_id: str) -> None:
    payload = _session_metadata_payload(session_id)
    if payload:
        await persist_session_metadata(session_id, payload)


def add_log(session_id: str, message: str):
    """Add a log message for a trading session"""
    with _state_lock:
        if session_id not in trading_logs:
            trading_logs[session_id] = []
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] {message}"
        trading_logs[session_id].append(log_entry)
    try:
        persist_log_sync(session_id, log_entry)
        # Logs must not snapshot stale worker RAM auth into Mongo (multi-worker 401 bug).
        persist_trading_runtime_sync(session_id)
    except Exception:
        # Best effort; avoid raising from logging
        pass
    # Don't use print here - it will cause infinite loop with monkey-patched print


class LogCapture:
    """Capture stdout and redirect to both console and web logs"""
    def __init__(self, session_id: str, original_stdout):
        self.session_id = session_id
        self.original_stdout = original_stdout
        self._in_write = False  # Prevent recursion
        # Copy attributes from original stdout for compatibility
        self.encoding = getattr(original_stdout, 'encoding', 'utf-8')
        self.errors = getattr(original_stdout, 'errors', 'replace')

    def write(self, text):
        # Prevent infinite recursion
        if self._in_write:
            if self.original_stdout:
                self.original_stdout.write(text)
            return

        self._in_write = True
        try:
            # Write to original stdout (console/screen)
            if self.original_stdout:
                self.original_stdout.write(text)
                self.original_stdout.flush()

            # Also capture to web logs (skip empty lines)
            if text.strip() and self.session_id in trading_logs:
                timestamp = datetime.now().strftime("%H:%M:%S")
                trading_logs[self.session_id].append(f"[{timestamp}] {text.strip()}")
        finally:
            self._in_write = False

    def flush(self):
        if self.original_stdout:
            self.original_stdout.flush()

    def isatty(self):
        return False
