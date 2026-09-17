"""Mintzy plugin — FastAPI application assembly.

This module only wires the app together: it creates the FastAPI instance,
registers middleware and routers, and keeps the local `python api_server.py`
entrypoint. All request logic lives in `routes/`, `services/`, `repositories/`,
and `session_manager.py`.

Gunicorn target (Dockerfile): `api_server:app`.
"""
from datetime import datetime

import uvicorn
from fastapi import FastAPI, Header
from fastapi.middleware.cors import CORSMiddleware

from core.middleware import RequestLoggingMiddleware
from routes import admin, auth, dashboard, debug, simulation, trading
from state import sessions_store

app = FastAPI(title="AutoTrader API", version="1.0.0")

app.add_middleware(RequestLoggingMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router)
app.include_router(trading.router)
app.include_router(simulation.router)
app.include_router(admin.router)
app.include_router(dashboard.router)
app.include_router(debug.router)


@app.get("/")
def root():
    return {"message": "Mintzy Plugin API"}


@app.get("/api/health")
async def health_check(x_plugin_api_key: str = Header(None)):
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "active_sessions": len(sessions_store)
    }


if __name__ == "__main__":
    import socket

    # Get hostname and IP
    hostname = socket.gethostname()
    try:
        local_ip = socket.gethostbyname(hostname)
    except Exception:
        local_ip = "localhost"

    print("=" * 80)
    print("AutoTrader FastAPI Server")
    print("=" * 80)
    print(f"\nServer: {hostname}")
    print(f"Local IP: {local_ip}")
    print("Port: 8000")
    print("\nAccess URLs:")
    print(f"   Local:     http://localhost:8000")
    print(f"   Network:   http://{local_ip}:8000")
    print(f"   VM/Public: http://34.56.133.232 (if deployed on VM)")
    print("\nAPI Documentation:")
    print(f"   Swagger:   http://localhost:8000/docs")
    print(f"   ReDoc:     http://localhost:8000/redoc")
    print("\nPress Ctrl+C to stop\n")
    print("=" * 80)

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
        access_log=True
    )
