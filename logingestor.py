"""
logingestor — Python SDK

Works with Python 3.8+ using only the standard library.

Minimal usage (reads LOGINGESTOR_API_KEY and LOGINGESTOR_PROJECT_ID from env)::

    import logingestor
    from fastapi import FastAPI          # or Flask

    app = FastAPI()
    client = logingestor.init(app, source="order-service")

    # stdlib logging is now wired to the ingestor automatically.
    # Use client.info / client.error for direct calls, or just use
    # logging.getLogger(__name__) — both go to the ingestor.
"""

from __future__ import annotations

import atexit
import json
import logging
import os
import ssl
import sys
import threading
import time
import traceback
import urllib.request
from datetime import datetime, timezone
from typing import Any, Callable, Optional


DEFAULT_BASE_URL = "https://api.streamlogia.com"


def _ssl_context() -> ssl.SSLContext:
    """Return an SSL context using certifi's CA bundle if available, else default."""
    try:
        import certifi  # noqa: PLC0415
        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        return ssl.create_default_context()


class Level:  # pylint: disable=too-few-public-methods
    """Log level constants."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"


# Maps Python stdlib logging levels to ingestor levels
_LOGGING_LEVEL_MAP = {
    logging.DEBUG:    Level.DEBUG,
    logging.INFO:     Level.INFO,
    logging.WARNING:  Level.WARN,
    logging.ERROR:    Level.ERROR,
    logging.CRITICAL: Level.ERROR,
}

_CONSOLE_MAP = {
    Level.DEBUG: sys.stdout,
    Level.INFO:  sys.stdout,
    Level.WARN:  sys.stderr,
    Level.ERROR: sys.stderr,
}


class LogIngestorClient:
    """
    Thread-safe client that batches log entries and flushes them to the
    Log Ingestor service in the background.

    :param api_key:         API key used for authentication
    :param project_id:      UUID of the project to ingest into
    :param source:          Default source tag on every entry (default: "unknown")
    :param batch_size:      Flush when the queue reaches this size (default: 1, sends every entry immediately)
    :param flush_interval:  Background flush interval in seconds (default: 5.0)
    :param console:         Mirror every log to stdout/stderr as well (default: True).
                            When False, logs go to the ingestor only — nothing will
                            appear in the terminal or journalctl on your server machine.
    :param on_error:        Called with the exception when an ingest request fails.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        project_id: Optional[str] = None,
        *,
        source: str = "unknown",
        batch_size: int = 1,
        flush_interval: float = 5.0,
        console: bool = True,
        on_error: Optional[Callable[[Exception], None]] = None,
    ) -> None:
        api_key = api_key or os.environ.get("LOGINGESTOR_API_KEY")
        project_id = project_id or os.environ.get("LOGINGESTOR_PROJECT_ID")
        if not api_key:
            raise ValueError(
                "api_key is required. Pass it explicitly or set LOGINGESTOR_API_KEY."
            )
        if not project_id:
            raise ValueError(
                "project_id is required. Pass it explicitly or set LOGINGESTOR_PROJECT_ID."
            )
        self._base_url = DEFAULT_BASE_URL.rstrip("/")
        self._api_key = api_key
        self._project_id = project_id
        self._source = source
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._console = console
        self._on_error = on_error or (lambda e: print(
            f"[logingestor] {e}", file=sys.stderr))

        self._queue: list[dict] = []
        self._lock = threading.Lock()
        self._stop_event = threading.Event()

        self._timer_thread = threading.Thread(
            target=self._background_flusher, daemon=True)
        self._timer_thread.start()

    # ── Level helpers ─────────────────────────────────────────────────────────

    def debug(self, message: str, *, meta: Optional[dict] = None, tags: Optional[list[str]] = None) -> None:
        """Log a DEBUG-level message."""
        self._enqueue(Level.DEBUG, message, meta=meta, tags=tags)

    def info(self, message: str, *, meta: Optional[dict] = None, tags: Optional[list[str]] = None) -> None:
        """Log an INFO-level message."""
        self._enqueue(Level.INFO, message, meta=meta, tags=tags)

    def warn(self, message: str, *, meta: Optional[dict] = None, tags: Optional[list[str]] = None) -> None:
        """Log a WARN-level message."""
        self._enqueue(Level.WARN, message, meta=meta, tags=tags)

    def error(self, message: str, *, meta: Optional[dict] = None, tags: Optional[list[str]] = None) -> None:
        """Log an ERROR-level message."""
        self._enqueue(Level.ERROR, message, meta=meta, tags=tags)

    # ── Direct send ───────────────────────────────────────────────────────────

    def ingest(self, entries: list[dict]) -> dict:
        """
        Send a list of entries immediately, bypassing the internal queue.
        Returns the server response: {"ingested": N, "ids": [...]}.
        Raises on HTTP errors.
        """
        body = json.dumps(entries).encode()
        req = urllib.request.Request(
            f"{self._base_url}/v1/ingest",
            data=body,
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10, context=_ssl_context()) as resp:
            return json.loads(resp.read())

    def flush(self) -> None:
        """Drain the internal queue immediately."""
        with self._lock:
            batch = self._queue[:]
            self._queue.clear()

        if not batch:
            return
        try:
            self.ingest(batch)
        except Exception as exc:
            self._on_error(exc)

    def close(self) -> None:
        """Flush pending logs and stop the background thread."""
        self._stop_event.set()
        self._timer_thread.join(timeout=self._flush_interval + 2)
        self.flush()

    # ── Flask integration ─────────────────────────────────────────────────────

    def flask_middleware(self, app: Any) -> Any:
        """
        Register before/after request hooks on a Flask app.
        One log entry is produced per request after the response is sent.

        Usage::

            from flask import Flask
            app = Flask(__name__)
            client.flask_middleware(app)
        """
        @app.before_request
        def _before():
            # pylint: disable=import-outside-toplevel,import-error
            from flask import g  # noqa: PLC0415
            g.logingestor_start = time.monotonic()

        @app.after_request
        def _after(response):
            # pylint: disable=import-outside-toplevel,import-error
            from flask import g, request  # noqa: PLC0415
            duration_ms = int(
                (time.monotonic() - g.logingestor_start) * 1000)
            status = response.status_code
            meta = {
                "method": request.method,
                "path": request.path,
                "status": status,
                "duration_ms": duration_ms,
                "user_agent": request.user_agent.string,
                "ip": request.remote_addr,
            }
            if request.headers.get("X-Request-Id"):
                meta["request_id"] = request.headers["X-Request-Id"]

            level = _level_for_status(status)
            msg = f"{request.method} {request.path} {status} ({duration_ms}ms)"
            self._enqueue(level, msg, meta=meta)
            return response

        return app

    # ── FastAPI / Starlette ASGI middleware ───────────────────────────────────

    def asgi_middleware(self) -> type:
        """
        Returns a Starlette-compatible ASGI middleware class.

        Usage::

            from fastapi import FastAPI
            app = FastAPI()
            app.add_middleware(client.asgi_middleware())
        """
        client = self

        # pylint: disable=import-outside-toplevel,import-error
        # type: ignore[import-not-found]
        from starlette.middleware.base import BaseHTTPMiddleware
        # type: ignore[import-not-found]
        from starlette.requests import Request
        # pylint: enable=import-outside-toplevel,import-error

        class LogIngestorMiddleware(BaseHTTPMiddleware):
            async def dispatch(self, request: Request, call_next):
                start = time.monotonic()
                response = await call_next(request)
                duration_ms = int((time.monotonic() - start) * 1000)
                status = response.status_code
                meta = {
                    "method": request.method,
                    "path": request.url.path,
                    "status": status,
                    "duration_ms": duration_ms,
                    "user_agent": request.headers.get("user-agent"),
                    "ip": request.client.host if request.client else None,
                }
                if request.headers.get("x-request-id"):
                    meta["request_id"] = request.headers["x-request-id"]

                level = _level_for_status(status)
                msg = f"{request.method} {request.url.path} {status} ({duration_ms}ms)"
                client._enqueue(level, msg, meta=meta)
                return response

        return LogIngestorMiddleware

    # ── stdlib logging integration ────────────────────────────────────────────

    def logging_handler(self) -> logging.Handler:
        """
        Returns a logging.Handler that forwards all stdlib log records to the
        ingestor. Plug it into any logger.

        Usage::

            import logging
            logger = logging.getLogger("myapp")
            logger.addHandler(client.logging_handler())
            logger.setLevel(logging.DEBUG)

            logger.info("order created", extra={"meta": {"order_id": "o_1"}})
        """
        return _LogIngestorHandler(self)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _enqueue(
        self,
        level: str,
        message: str,
        *,
        meta: Optional[dict] = None,
        tags: Optional[list[str]] = None,
        source: Optional[str] = None,
    ) -> None:
        if self._console:
            stream = _CONSOLE_MAP.get(level, sys.stdout)
            print(f"[{level}] {message}", meta or {}, file=stream, flush=True)

        entry = {
            "projectId": self._project_id,
            "level": level,
            "message": message,
            "source": source or self._source,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tags": tags or [],
            "meta": meta or {},
        }

        with self._lock:
            self._queue.append(entry)
            should_flush = len(self._queue) >= self._batch_size

        if should_flush:
            threading.Thread(target=self.flush, daemon=True).start()

    def _background_flusher(self) -> None:
        while not self._stop_event.wait(timeout=self._flush_interval):
            self.flush()


class _LogIngestorHandler(logging.Handler):
    """logging.Handler that forwards records to the ingestor."""

    def __init__(self, client: LogIngestorClient) -> None:
        super().__init__()
        self._client = client

    def emit(self, record: logging.LogRecord) -> None:
        level = _LOGGING_LEVEL_MAP.get(record.levelno, Level.INFO)
        meta: dict[str, Any] = {}

        # Capture extra fields passed via logger.info(..., extra={"meta": {...}})
        if hasattr(record, "meta") and isinstance(record.meta, dict):
            meta = record.meta

        # Always include exception info if present
        if record.exc_info:
            import traceback
            meta["exception"] = "".join(
                traceback.format_exception(*record.exc_info))

        self._client._enqueue(level, self.format(record), meta=meta)


def init(
    app: Any = None,
    *,
    source: str = "unknown",
    batch_size: int = 1,
    flush_interval: float = 5.0,
    console: bool = True,
    log_level: int = logging.DEBUG,
    api_key: Optional[str] = None,
    project_id: Optional[str] = None,
    on_error: Optional[Callable[[Exception], None]] = None,
) -> LogIngestorClient:
    """
    One-call setup for Flask and FastAPI/Starlette apps.

    - Reads ``LOGINGESTOR_API_KEY`` and ``LOGINGESTOR_PROJECT_ID`` from the
      environment (override with *api_key* / *project_id*).
    - Attaches request-logging middleware to *app* (pass ``None`` to skip).
    - Routes the stdlib root logger through the ingestor so every
      ``logging.getLogger(...)`` call is captured automatically.
    - Registers a shutdown hook to flush buffered logs on exit.

    Returns the :class:`LogIngestorClient` for direct calls (``client.info(...)``)
    or for passing to other parts of your application.

    Usage::

        # FastAPI
        app = FastAPI()
        client = logingestor.init(app, source="order-service")

        # Flask
        app = Flask(__name__)
        client = logingestor.init(app, source="payment-service")

        # No framework — just stdlib logging integration
        client = logingestor.init(source="worker")
    """
    client = LogIngestorClient(
        api_key=api_key,
        project_id=project_id,
        source=source,
        batch_size=batch_size,
        flush_interval=flush_interval,
        console=console,
        on_error=on_error,
    )

    # Wire stdlib root logger so every logging.getLogger(...) goes to ingestor.
    root = logging.getLogger()
    root.setLevel(log_level)
    root.addHandler(client.logging_handler())

    if app is not None:
        if hasattr(app, "add_middleware"):
            # FastAPI / Starlette
            app.add_middleware(client.asgi_middleware())
            app.add_event_handler("shutdown", client.close)
        elif hasattr(app, "before_request"):
            # Flask
            client.flask_middleware(app)
            atexit.register(client.close)
    else:
        atexit.register(client.close)

    return client


def _level_for_status(status: int) -> str:
    if status >= 500:
        return Level.ERROR
    if status >= 400:
        return Level.WARN
    return Level.INFO
