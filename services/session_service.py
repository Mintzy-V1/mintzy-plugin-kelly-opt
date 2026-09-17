"""Session retrieval / restoration logic shared across auth, trading, and simulation."""
from typing import Any, Dict, Optional

from starlette.concurrency import run_in_threadpool

from broker_angle import BrokerConnector
from core.logging import get_logger
from repositories.db import fetch_session_from_db
from state import sessions_store

_session_log = get_logger("SESSION")


async def get_or_restore_session(session_id: str) -> Optional[Dict[str, Any]]:
    """
    Robust session retrieval that handles multi-worker synchronization.
    1. Checks in-memory store.
    2. If in-memory is missing OR stale (not authenticated), checks DB.
    3. If DB has authenticated session, restores it into memory.
    """
    session_data = sessions_store.get(session_id)
    _session_log.info(
        "session=%s RESTORE_LOOKUP local_status=%s",
        session_id, session_data.get('status') if session_data else None,
    )

    # Fast path: If RAM has it and it's authenticated, return immediately
    if session_data and session_data.get("status") == "authenticated":
        # Additional safety: ensure broker object exists
        if session_data.get("broker") and session_data.get("broker_session"):
            from repositories.db import DB_CONNECTED
            if session_data.get("configuration_id") is None and DB_CONNECTED:
                db_session = await fetch_session_from_db(session_id)
                if db_session and db_session.get("configuration_id"):
                    session_data["configuration_id"] = db_session.get("configuration_id")
                    sessions_store[session_id] = session_data
            return session_data

    # Slow path: Fetch from DB if we can
    from repositories.db import DB_CONNECTED
    if not DB_CONNECTED:
        _session_log.info("session=%s RESTORE_DB_UNAVAILABLE", session_id)
        return session_data

    db_session = await fetch_session_from_db(session_id)

    if not db_session:
        _session_log.info("session=%s RESTORE_NOT_FOUND", session_id)
        return session_data

    db_status = db_session.get("status")
    _session_log.info("session=%s RESTORE_DB_STATUS status=%s", session_id, db_status)

    broker_session_db = db_session.get("broker_session")
    db_has_stored_auth = (
        db_session.get("api_key")
        and db_session.get("client_code")
        and db_session.get("password")
        and broker_session_db
        and (
            db_status == "authenticated"
            or db_session.get("authenticated_at")
        )
    )

    # If DB has auth tokens, restore when local RAM is missing or stale.
    if db_has_stored_auth:
        try:
            broker = BrokerConnector(
                require_totp=False,
                api_key=db_session["api_key"],
                client_code=db_session["client_code"],
                password=db_session["password"],
            )

            restored_session = await run_in_threadpool(broker.restore_session, {
                "token": broker_session_db.get("token"),
                "refresh_token": broker_session_db.get("refresh_token"),
                "feed_token": broker_session_db.get("feed_token"),
            })

            if restored_session and restored_session.get("token"):
                # Success! Update sessions_store
                sessions_store[session_id] = {
                    "session_id": session_id,
                    "api_key": db_session.get("api_key"),
                    "client_code": db_session.get("client_code"),
                    "password": db_session.get("password"),
                    "status": "authenticated",
                    "free_cash": db_session.get("free_cash"),
                    "broker_session": restored_session,
                    "broker": broker,
                    "authenticated_at": db_session.get("authenticated_at"),
                    "created_at": db_session.get("created_at"),
                    "user_id": db_session.get("user_id"),
                    "configuration_id": db_session.get("configuration_id"),
                }
                _session_log.info("session=%s RESTORE_SUCCESS from_db", session_id)
                return sessions_store[session_id]
        except Exception as e:
            import logging
            _session_log.warning("session=%s RESTORE_FAILED error=%s", session_id, e)
            logging.getLogger(__name__).error(f"[{session_id}] Failed to restore session from DB: {e}")

    # If we are here, we couldn't restore broker from DB tokens.
    # Populate RAM from DB when local is missing or clearly stale vs persisted auth markers.
    local_stale = (
        not session_data
        or (
            session_data.get("status") != "authenticated"
            and db_has_stored_auth
        )
    )
    if local_stale and db_session:
        # Restore all serializable fields so subsequent steps (like totp) can use them
        sessions_store[session_id] = {
            "session_id": session_id,
            "status": "authenticated" if db_has_stored_auth else db_session.get("status"),
            "created_at": db_session.get("created_at"),
            "api_key": db_session.get("api_key"),
            "client_code": db_session.get("client_code"),
            "password": db_session.get("password"),
            "user_id": db_session.get("user_id"),
            "free_cash": db_session.get("free_cash"),
            "broker_session": db_session.get("broker_session"),
            "authenticated_at": db_session.get("authenticated_at"),
            "configuration_id": db_session.get("configuration_id"),
        }
        return sessions_store[session_id]

    return session_data
