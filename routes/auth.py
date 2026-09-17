"""Authentication + session status routes."""
import logging
import os
import threading
from datetime import datetime

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

from broker_angle import BrokerConnector
from core.config import _encrypt_val
from core.helpers import guess_free_cash_from_resp
from core.logging import get_logger, redact
from models import BrokerCredentials, TOTPRequest
from repositories.db import fetch_session_from_db, persist_session_metadata, persist_session_state
from services.session_service import get_or_restore_session
from state import sessions_store

logger = logging.getLogger(__name__)
_auth_log = get_logger("AUTH")

router = APIRouter()


@router.post("/api/auth/credentials")
async def authenticate_credentials(credentials: BrokerCredentials, request: Request, x_plugin_api_key: str = Header(None), x_forwarded_user: str = Header(None)):
    """
    Step 1: Accept and validate broker credentials.
    Returns session_id for next step.
    """
    if not x_forwarded_user:
        raise HTTPException(status_code=400, detail="Missing X-Forwarded-User header")
    if not x_forwarded_user:
        raise HTTPException(status_code=400, detail="Missing X-Forwarded-User header")
    try:
        # Generate session ID
        session_id = f"session_{datetime.now().strftime('%Y%m%d%H%M%S')}_{os.urandom(4).hex()}"
        # Store credentials temporarily
        sessions_store[session_id] = {
            "session_id": session_id,
            "user_id": x_forwarded_user,
            "api_key": credentials.api_key,
            "client_code": credentials.client_code,
            "password": credentials.password,
            "created_at": datetime.now().isoformat(),
            "status": "credentials_received"
        }
        await persist_session_state(session_id)

        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "session_id": session_id,
                "message": "Credentials received. Please provide TOTP.",
                "requires_totp": True
            }
        )

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Credentials validation failed: {str(e)}")


@router.post("/api/auth/totp")
async def authenticate_totp(totp_req: TOTPRequest, x_plugin_api_key: str = Header(None)):
    """
    Step 2: Complete authentication with TOTP.
    Establishes broker connection and returns account info.
    """
    session_id = totp_req.session_id

    # Use robust session retrieval
    session_data = await get_or_restore_session(session_id)

    if not session_data:
        raise HTTPException(status_code=404, detail="Session not found or expired")

    # Validate credentials exist before using them
    missing = [k for k in ("api_key", "client_code", "password") if not session_data.get(k)]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing credential fields in session ({', '.join(missing)}). Please re-enter credentials."
        )

    _auth_log.info(
        "session=%s TOTP_AUTH_STARTED pid=%s thread=%s",
        session_id, os.getpid(), threading.current_thread().name,
    )

    try:
        # Create broker connector with explicit credentials (no shared env mutation)
        logger.info(f"[{session_id}] Creating BrokerConnector instance...")
        broker = BrokerConnector(
            require_totp=True,
            api_key=session_data["api_key"],
            client_code=session_data["client_code"],
            password=session_data["password"],
            totp=totp_req.totp or "",
        )
        logger.info(f"[{session_id}] BrokerConnector created successfully")

        # Step 3: Get session from broker
        logger.info(f"[{session_id}] Calling broker.get_session()...")
        broker_session = await run_in_threadpool(broker.get_session)
        _auth_log.info(
            "session=%s BROKER_SESSION_CREATED user=%s has_token=%s",
            session_id,
            broker_session.get("user") if isinstance(broker_session, dict) else None,
            bool(isinstance(broker_session, dict) and broker_session.get("token")),
        )
        logger.info(f"[{session_id}] broker_session received: {type(broker_session)}")
        logger.info(f"[{session_id}] broker_session keys: {list(broker_session.keys()) if isinstance(broker_session, dict) else 'Not a dict'}")

        if not broker_session or "token" not in broker_session:
            logger.error(f"[{session_id}] broker_session invalid: {redact(broker_session)}")
            raise Exception("Failed to establish broker session")
        logger.info(f"[{session_id}] broker_session validated successfully")

        # Step 4: Get account balance
        logger.info(f"[{session_id}] Calling broker.get_account_balance()...")
        balance_resp = await run_in_threadpool(broker.get_account_balance, broker_session)
        _auth_log.info(
            "session=%s ACCOUNT_BALANCE_RECEIVED type=%s", session_id, type(balance_resp).__name__
        )
        free_cash = None
        if isinstance(balance_resp, dict) and balance_resp.get("status") == "success":
            free_cash = balance_resp.get("free_cash")
            logger.info(f"[{session_id}] free_cash from response: {free_cash}")
            if free_cash is None:
                logger.info(f"[{session_id}] Guessing free_cash from response...")
                free_cash = guess_free_cash_from_resp(balance_resp)
                logger.info(f"[{session_id}] Guessed free_cash: {free_cash}")
        else:
            logger.warning(f"[{session_id}] balance_resp not successful or not dict")

        # Step 5: Update session store
        logger.info(f"[{session_id}] Updating session store...")
        sessions_store[session_id].update({
            "status": "authenticated",
            "broker": broker,
            "broker_session": broker_session,
            "free_cash": free_cash,
            "authenticated_at": datetime.now().isoformat()
        })
        logger.info(f"[{session_id}] Session store updated")

        # Step 6: Persist to DB
        logger.info(f"[{session_id}] Persisting session state to DB...")
        await persist_session_state(session_id)
        logger.info(f"[{session_id}] Session state persisted")

        logger.info(f"[{session_id}] TOTP authentication completed successfully")

        # HARD persist authenticated state (do NOT rely on persist_session_state)
        await persist_session_metadata(
            session_id,
            {
                "status": "authenticated",
                "authenticated_at": datetime.utcnow(),
                "free_cash": free_cash,
                "broker_session": {
                    "user": broker_session.get("user"),
                    "token": _encrypt_val(broker_session.get("token")),
                    "refresh_token": _encrypt_val(broker_session.get("refresh_token")),
                    "feed_token": _encrypt_val(broker_session.get("feed_token")),
                }
            }
        )

        db_session = fetch_session_from_db(session_id=session_id)
        _auth_log.info("session=%s TOTP_AUTH_COMPLETE free_cash=%s", session_id, free_cash)

        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "session_id": session_id,
                "message": "Authentication successful",
                "account_info": {
                    "user": broker_session.get("user"),
                    "free_cash": free_cash,
                    "balance_details": balance_resp if isinstance(balance_resp, dict) else None
                },
                "free_cash": free_cash,
            }
        )

    except Exception as e:
        logger.error(f"[{session_id}] Exception in TOTP authentication: {type(e).__name__}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=401, detail=f"Authentication failed: {str(e)}")


@router.get("/api/session/{session_id}/status")
async def get_session_status(session_id: str, x_plugin_api_key: str = Header(None)):
    """Check session status and account info."""

    # Use robust getter to sync status
    session_data = await fetch_session_from_db(session_id)
    _auth_log.info("session=%s SESSION_STATUS_QUERY found=%s", session_id, bool(session_data))

    if not session_data:
        raise HTTPException(status_code=404, detail="Session not found")

    return {
        "session_id": session_id,
        "status": session_data.get("status"),
        "created_at": session_data.get("created_at"),
        "authenticated_at": session_data.get("authenticated_at"),
        "free_cash": session_data.get("free_cash")
    }
