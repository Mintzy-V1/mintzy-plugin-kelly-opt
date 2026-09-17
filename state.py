"""Shared in-memory runtime state for the plugin API.

These dicts previously lived as module-level globals in ``api_server.py``.
They are process-local (one copy per gunicorn worker) and are the source of
truth for RAM session/auth/trading status between requests.
"""
import threading
from typing import Dict, Optional

# Guards compound read-modify-write on the shared stores below.
_state_lock = threading.RLock()

# session_id -> session/auth info (broker object, credentials, status, ...)
sessions_store: Dict[str, Dict] = {}

# session_id -> list of web-facing log strings (mirrored to Mongo)
trading_logs: Dict[str, list] = {}

# session_id -> trading runtime status info
trading_status: Dict[str, dict] = {}

# Track current active session for stdout capture
active_session_id: Optional[str] = None
