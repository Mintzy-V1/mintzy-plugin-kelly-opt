"""Structured, component-scoped logging for the Mintzy plugin.

Logs go to a dedicated ``mintzy`` logger namespace so they never collide with
uvicorn/gunicorn access logs or the application's own ``logging.getLogger`` uses.

Every line carries: TIMESTAMP | LEVEL | COMPONENT | req=<request_id>? | message
so VM logs stay grep-able by session, component, and request.
"""
import logging
import sys
from contextvars import ContextVar

LOG_NAMESPACE = "mintzy"

# Request-scoped correlation id, set by the request-id middleware (if installed).
REQUEST_ID: ContextVar = ContextVar("mintzy_request_id", default=None)


class StructuredFormatter(logging.Formatter):
    def format(self, record):
        timestamp = self.formatTime(record, "%Y-%m-%d %H:%M:%S")
        component = getattr(record, "component", None) or record.name
        request_id = REQUEST_ID.get()
        head = f"{timestamp} | {record.levelname:<7} | {component}"
        if request_id:
            head += f" | req={request_id}"
        message = record.getMessage()
        if record.exc_info and not message.endswith(str(record.exc_info[1] or "")):
            message = f"{message} | {self.formatException(record.exc_info)}"
        return f"{head} | {message}"


def _ensure_handler() -> None:
    root = logging.getLogger(LOG_NAMESPACE)
    if root.handlers:
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(StructuredFormatter())
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    root.propagate = False


class ComponentAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        extra = dict(kwargs.get("extra") or {})
        extra.setdefault("component", self.extra.get("component"))
        kwargs["extra"] = extra
        return msg, kwargs


def get_logger(component: str) -> logging.LoggerAdapter:
    """Return a component logger whose lines are prefixed with ``component``."""
    _ensure_handler()
    base = logging.getLogger(LOG_NAMESPACE)
    return ComponentAdapter(base, {"component": component})


def set_request_id(request_id: str | None) -> None:
    REQUEST_ID.set(request_id)


def install_stdout_logging(component: str = "WORKER"):
    """Redirect this process's stdout through the structured logger.

    Used in trader worker subprocesses so every `print()` (including the trading
    engines' `[TAG]` diagnostics) becomes a structured log line. The logging
    handler is repointed at the *original* stdout to avoid recursion.
    """
    import sys

    real = sys.stdout
    log = get_logger(component)

    root = logging.getLogger(LOG_NAMESPACE)
    for handler in root.handlers:
        if isinstance(handler, logging.StreamHandler):
            handler.stream = real

    class _StdoutToLogger:
        def write(self, text):
            if text:
                for line in text.splitlines():
                    if line.strip():
                        log.info("%s", line)
            return len(text)

        def flush(self):
            try:
                real.flush()
            except Exception:
                pass

        def isatty(self):
            return False

    sys.stdout = _StdoutToLogger()
    return real


# Keys/values never to appear in log output. Used by `redact`.
_SECRET_KEYS = frozenset({
    "password", "api_key", "apikey", "client_code", "token", "jwt_token",
    "jwtToken", "access_token", "refresh_token", "refreshToken", "feed_token",
    "feedToken", "totp", "secret", "authorization", "x_plugin_api_key",
})

_SECRET_VALUE_PREFIXES = ("Bearer ",)


def redact(value):
    """Return a safe copy of a dict/list with secret fields masked.

    Never mutates the input. Values for secret keys are replaced by ``***``.
    """
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            key = str(k)
            if key in _SECRET_KEYS or key.lower() in _SECRET_KEYS:
                out[k] = "***"
            else:
                out[k] = redact(v)
        return out
    if isinstance(value, (list, tuple)):
        return [redact(v) for v in value]
    return value
