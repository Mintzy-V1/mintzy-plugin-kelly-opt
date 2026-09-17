"""Request-logging middleware: request-id correlation + lifecycle logging.

Pure ASGI middleware (no BaseHTTPMiddleware) so it cannot interfere with
StreamingResponse bodies or background tasks. It adds no response headers and
changes no response — it only emits one structured log line per HTTP request.
"""
import time
import uuid

from core.logging import get_logger, set_request_id

_api_log = get_logger("API")


class RequestLoggingMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers") or [])
        request_id = headers.get(b"x-request-id")
        if isinstance(request_id, bytes):
            request_id = request_id.decode("latin-1")
        if not request_id:
            request_id = uuid.uuid4().hex[:12]

        set_request_id(request_id)
        started = time.perf_counter()

        status_holder = {"status": None}

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                status_holder["status"] = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            duration_ms = round((time.perf_counter() - started) * 1000, 2)
            _api_log.info(
                "REQUEST method=%s path=%s status=%s duration_ms=%s",
                scope.get("method"),
                scope.get("path"),
                status_holder["status"],
                duration_ms,
            )
            set_request_id(None)
