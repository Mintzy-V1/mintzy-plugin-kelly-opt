"""Plugin-key dependency used by a subset of admin/debug endpoints."""
from fastapi import Header, HTTPException

from core.config import SECRET


async def verify_plugin_key(x_plugin_api_key: str = Header(default=None)):
    if x_plugin_api_key != SECRET:
        from core.logging import get_logger
        get_logger("AUTH").warning("PLUGIN_KEY_REJECTED")
        raise HTTPException(status_code=401, detail="Invalid plugin key")
