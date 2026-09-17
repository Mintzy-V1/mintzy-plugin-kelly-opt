"""Environment and filesystem configuration for the Mintzy plugin.

Centralizes the module-level constants previously scattered at the top of
``api_server.py``. Values are unchanged so behavior is preserved.
"""
import base64
import hashlib
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv

load_dotenv()

try:
    from cryptography.fernet import Fernet
except Exception:
    Fernet = None

MINTZY_SECRET_KEY = os.getenv("MINTZY_SECRET_KEY")
SECRET = os.getenv("SECRET")

# File system paths. BASE_DIR is the repo root (config.py lives in core/).
BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_LOGS_DIR = BASE_DIR / "logs"
VM_LOGS_DIR = Path("/home/jiyachaturvedi24/mintzy_plugin_files/logs")

REMOTE_DASHBOARD_BASE = os.environ.get("MINTZY_REMOTE_DASHBOARD_URL", "").strip()
REMOTE_DASHBOARD_TIMEOUT = float(os.environ.get("MINTZY_REMOTE_TIMEOUT", "20"))
REMOTE_DASHBOARD_CACHE_SECONDS = int(os.environ.get("MINTZY_REMOTE_CACHE_SECONDS", "20"))
REMOTE_DASHBOARD_FAILURE_COOLDOWN = int(os.environ.get("MINTZY_REMOTE_FAILURE_COOLDOWN", "30"))

# Cache remote dashboard payloads to avoid repeated slow calls
REMOTE_DASHBOARD_CACHE: Dict[str, Dict[str, Any]] = {}
REMOTE_DASHBOARD_FAILURES: Dict[str, datetime] = {}

MONGO_URI = os.environ.get("MONGO_URI", "").strip()

MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "mintzy_plugin")
MONGO_CONFIG_DB_NAME = os.environ.get("MONGO_CONFIG_DB_NAME", MONGO_DB_NAME)


_FERNET = None
if MINTZY_SECRET_KEY and Fernet:
    try:
        # derive a 32-byte urlsafe base64 key if user provided a passphrase
        key = MINTZY_SECRET_KEY
        if len(key) != 44:
            key = base64.urlsafe_b64encode(hashlib.sha256(key.encode()).digest()).decode()
        _FERNET = Fernet(key.encode())
    except Exception:
        _FERNET = None


def _encrypt_val(v):
    if v is None:
        return None
    s = str(v)
    if _FERNET:
        try:
            return _FERNET.encrypt(s.encode()).decode()
        except Exception:
            return None
    return s


def _decrypt_val(v):
    if v is None:
        return None
    if _FERNET:
        try:
            return _FERNET.decrypt(v.encode()).decode()
        except Exception:
            # Fallback: assume unencrypted (legacy/corrupted data) and return as-is
            return v
    return v


def resolve_logs_dir() -> Path:
    """Determine which logs directory to use (env override > VM path > local logs)."""
    env_dir = os.environ.get("MINTZY_LOGS_DIR")
    if env_dir:
        return Path(env_dir)
    if VM_LOGS_DIR.exists():
        return VM_LOGS_DIR
    return DEFAULT_LOGS_DIR


LOGS_DIR = resolve_logs_dir()
