"""Dashboard routes (local CSV/file reads + optional remote proxy)."""
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import FileResponse

from core.config import (
    LOGS_DIR,
    REMOTE_DASHBOARD_BASE,
    REMOTE_DASHBOARD_CACHE,
    REMOTE_DASHBOARD_CACHE_SECONDS,
    REMOTE_DASHBOARD_FAILURE_COOLDOWN,
    REMOTE_DASHBOARD_FAILURES,
    REMOTE_DASHBOARD_TIMEOUT,
)
from core.helpers import _limit_rows, read_csv_data

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/api/dashboard/live-data/download")
def download_live_data():
    file_path = LOGS_DIR / "live_data.csv"

    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Live data not available")

    return FileResponse(
        path=file_path,
        filename="live_data.csv",
        media_type="text/csv",
    )


def fetch_remote_dashboard_rows(path: str, limit: int) -> Optional[List[Dict[str, Any]]]:
    """
    Fetch rows from a remote dashboard service.
    SAFE: will never self-call in production.
    """
    if not REMOTE_DASHBOARD_BASE:
        return None

    if os.getenv("ENV") == "production":
        return None

    cache_key = path.strip("/")
    now = datetime.utcnow()

    cache_entry = REMOTE_DASHBOARD_CACHE.get(cache_key)
    if cache_entry:
        age = (now - cache_entry["timestamp"]).total_seconds()
        if age < REMOTE_DASHBOARD_CACHE_SECONDS:
            return _limit_rows(cache_entry["rows"], limit)

    last_failure = REMOTE_DASHBOARD_FAILURES.get(cache_key)
    if last_failure and (now - last_failure).total_seconds() < REMOTE_DASHBOARD_FAILURE_COOLDOWN:
        if cache_entry:
            return _limit_rows(cache_entry["rows"], limit)
        return None

    try:
        base = REMOTE_DASHBOARD_BASE.rstrip("/") + "/"
        url = urljoin(base, path.lstrip("/"))

        response = requests.get(
            url,
            params={"limit": limit},
            timeout=REMOTE_DASHBOARD_TIMEOUT,
        )
        response.raise_for_status()

        data = response.json()
        if isinstance(data, dict) and data.get("success") and isinstance(data.get("rows"), list):
            rows = data["rows"]
            REMOTE_DASHBOARD_CACHE[cache_key] = {
                "rows": rows,
                "timestamp": now,
            }
            return _limit_rows(rows, limit)

    except requests.exceptions.ReadTimeout as exc:
        logger.warning(
            "Remote dashboard read timeout for %s (timeout=%s): %s",
            path,
            REMOTE_DASHBOARD_TIMEOUT,
            exc,
        )
        REMOTE_DASHBOARD_FAILURES[cache_key] = now

        if cache_entry:
            return _limit_rows(cache_entry["rows"], limit)

    except Exception as exc:
        logger.warning("Remote dashboard fetch failed for %s: %s", path, exc)
        REMOTE_DASHBOARD_FAILURES[cache_key] = now

        if cache_entry:
            return _limit_rows(cache_entry["rows"], limit)

    return None


@router.get("/api/dashboard/trade-log")
async def get_trade_log(limit: int = 100):
    """
    Expose recent rows from logs/trade_log.csv
    SAFE: no remote self-call
    """
    file_path = LOGS_DIR / "trade_log.csv"
    rows = read_csv_data(file_path, limit)
    return {
        "success": True,
        "rows": rows,
        "count": len(rows),
        "source": "local",
    }


@router.get("/api/dashboard/portfolio-log")
async def get_portfolio_log(limit: int = 100):
    """
    Expose recent rows from logs/portfolio_log.csv
    SAFE: no remote self-call
    """
    file_path = LOGS_DIR / "portfolio_log.csv"
    rows = read_csv_data(file_path, limit)
    return {
        "success": True,
        "rows": rows,
        "count": len(rows),
        "source": "local",
    }
