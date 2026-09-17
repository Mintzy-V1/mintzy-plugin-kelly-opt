"""Debug/inspection routes."""
import random
from datetime import datetime

from fastapi import APIRouter, Header, HTTPException
from starlette.concurrency import run_in_threadpool

from broker_angle import BrokerConnector
from core.logging import get_logger, redact
from repositories.db import fetch_session_from_db, trading_logs_collection
from state import sessions_store
from trading_snapshot import insert_trading_snapshot
from trading_state import trading_snapshot

router = APIRouter()
_debug_log = get_logger("API")


@router.get("/api/debug/snapshot")
async def debug_snapshot():
    return {
        "active_sessions": list(trading_snapshot.keys()),
        "snapshots": {
            sid: {
                "cycle": data["cycle"],
                "timestamp": data["timestamp"],
                "symbols": len(data["symbols"]),
                "total_equity": data["total_equity"],
            }
            for sid, data in trading_snapshot.items()
        }
    }


@router.get("/api/debug/insert-fake-trades/{session_id}")
def debug_insert_fake_trades(session_id: str):
    """
    DEBUG ONLY:
    Inserts fake trading snapshots into MongoDB
    without running AutoTrader or broker.
    """

    fake_cycles = 3
    symbols = ["RELIANCE", "TCS", "INFY"]

    for cycle in range(1, fake_cycles + 1):
        rows = []

        for sym in symbols:
            rows.append({
                "symbol": sym,
                "curr_price": round(random.uniform(1000, 3000), 2),
                "return_pct": round(random.uniform(-2, 2), 4),
                "side": random.choice(["BUY", "SELL", "NONE"]),
                "signal": random.choice(["BUY", "SELL", "HOLD", "STOP-LOSS"]),
                "action": random.choice(["OPEN", "HOLD", "CLOSE"]),
                "unrealized_pnl": round(random.uniform(-500, 500), 2),
            })

        snapshot = {
            "cycle": cycle,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "cash_balance": 100000.0,
            "realized_pnl": round(random.uniform(-2000, 2000), 2),
            "unrealized_pnl": round(random.uniform(-1000, 1000), 2),
            "total_equity": 100000.0,
            "symbols": rows,
        }

        insert_trading_snapshot(
            trading_logs_collection=trading_logs_collection,
            session_id=session_id,
            cycle=cycle,
            snapshot=snapshot,
            rows=rows,
            simulation_logs=False,
        )

    return {
        "success": True,
        "message": "Fake trading logs inserted",
        "session_id": session_id,
        "cycles": fake_cycles,
        "symbols": symbols,
    }


@router.get("/api/debug/restore-session/{session_id}")
async def debug_restore_session(session_id: str, x_plugin_api_key: str = Header(None)):
    """
    Debug-only: Validate that broker session restoration is working.
    This does NOT place orders or require TOTP again.
    """
    print("\n--- DEBUG: RESTORE SESSION CHECK ---")
    print("Session ID:", session_id)

    print("Fetching session from DB...")
    db_session = await fetch_session_from_db(session_id)
    if not db_session:
        raise HTTPException(status_code=404, detail="Session not found in DB")

    broker_session_db = db_session.get("broker_session")
    if not broker_session_db:
        raise HTTPException(status_code=400, detail="Broker session missing in DB")

    print("Broker session found in DB:")
    print({
        "has_token": bool(broker_session_db.get("token")),
        "has_refresh_token": bool(broker_session_db.get("refresh_token")),
        "has_feed_token": bool(broker_session_db.get("feed_token"))
    })

    print("Recreating broker for restore test...")
    res_broker = BrokerConnector(
        require_totp=False,
        api_key=db_session["api_key"],
        client_code=db_session["client_code"],
        password=db_session["password"],
    )

    print("Restoring session...")
    restored_session = await run_in_threadpool(res_broker.restore_session, {
        "token": broker_session_db.get("token"),
        "refresh_token": broker_session_db.get("refresh_token"),
        "feed_token": broker_session_db.get("feed_token"),
    })

    _debug_log.info(
        "session=%s RESTORE_TEST result=%s",
        session_id, redact(restored_session),
    )

    print("Testing account balance...")
    balance_test = await run_in_threadpool(res_broker.get_account_balance, restored_session)

    print("--- END DEBUG RESTORE CHECK ---\n")

    return {
        "success": True,
        "session_valid": bool(restored_session.get("token")),
        "user": restored_session.get("user"),
        "balance_status": balance_test.get("status"),
        "free_cash": balance_test.get("free_cash"),
        "raw_balance_check": balance_test.get("source")
    }


@router.get("/api/debug/sessions")
async def debug_sessions():
    """Debug endpoint to see active sessions (remove in production)"""
    return {
        "active_sessions": len(sessions_store),
        "session_ids": list(sessions_store.keys()),
        "session_statuses": {
            sid: {
                "status": data.get("status"),
                "created_at": data.get("created_at"),
                "authenticated_at": data.get("authenticated_at")
            }
            for sid, data in sessions_store.items()
        }
    }
