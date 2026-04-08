# logingestor — Python SDK

Thread-safe Python client for the Log Ingestor service. Batches and ships log entries in the background while optionally mirroring them to the console. Requires Python 3.8+ and no third-party dependencies.

## Installation

```bash
pip install streamlogia
```

## Quick start

```python
import os
from logingestor import LogIngestorClient

client = LogIngestorClient(
    api_key=os.environ["LOGINGESTOR_API_KEY"],
    project_id=os.environ["LOGINGESTOR_PROJECT_ID"],
    source="order-service",
)

client.info("user signed in", meta={"user_id": "u_123"})
client.warn("rate limit approaching", tags=["alerts"])
client.error("payment failed", meta={"order_id": "o_456", "reason": "card_declined"})

client.close()  # flush remaining logs before exit
```

By default every log is sent to both the ingestor **and** printed to stdout/stderr. Pass `console=False` to suppress console output.

## Constructor options

| Parameter        | Type       | Default          | Description                                              |
| ---------------- | ---------- | ---------------- | -------------------------------------------------------- |
| `api_key`        | `str`      | required         | Your Log Ingestor API key                                |
| `project_id`     | `str`      | required         | UUID of the project to ingest into                       |
| `source`         | `str`      | `"unknown"`      | Default source tag applied to every entry                |
| `batch_size`     | `int`      | `1`              | Flush the queue when it reaches this many entries        |
| `flush_interval` | `float`    | `5.0`            | Background flush interval in seconds                     |
| `console`        | `bool`     | `True`           | Mirror logs to stdout/stderr in addition to the ingestor |
| `on_error`       | `callable` | prints to stderr | Called with the exception when an ingest request fails   |

## Logging methods

```python
client.debug("cache miss", meta={"key": "user:99"})
client.info("order created", meta={"order_id": "o_1"}, tags=["orders"])
client.warn("disk usage high", meta={"used_pct": 87})
client.error("db connection failed", meta={"host": "db-1"})
```

All methods accept:

- `meta` — arbitrary `dict` of structured fields attached to the entry
- `tags` — list of string tags for filtering in the UI

## Console output

`DEBUG` and `INFO` go to **stdout**; `WARN` and `ERROR` go to **stderr**.

When running under systemd, stdout/stderr are captured by the journal:

```bash
journalctl -u your-service -f
```

Set `console=False` if you only want logs in the ingestor and nothing on the terminal.

## Flask integration

Automatically logs every HTTP request after the response is sent:

```python
from flask import Flask
app = Flask(__name__)
client.flask_middleware(app)
```

Each entry includes `method`, `path`, `status`, `duration_ms`, `ip`, `user_agent`, and `request_id` (when the `X-Request-Id` header is present). Status codes ≥ 500 are logged at `ERROR`, ≥ 400 at `WARN`, everything else at `INFO`.

See [`examples/flask_app.py`](examples/flask_app.py) for a complete example.

## FastAPI / Starlette integration

`asgi_middleware()` returns a Starlette-compatible middleware class:

```python
from fastapi import FastAPI
app = FastAPI()
app.add_middleware(client.asgi_middleware())
```

The same fields are captured as with the Flask middleware.

See [`examples/fastapi_app.py`](examples/fastapi_app.py) for a complete example including lifespan-based shutdown.

## stdlib `logging` integration

`logging_handler()` returns a `logging.Handler` that routes all stdlib log records through the client:

```python
import logging

logger = logging.getLogger("myapp")
logger.setLevel(logging.DEBUG)
logger.addHandler(client.logging_handler())

logger.info("server started")
logger.error("unhandled exception", exc_info=True)  # exception traceback captured in meta

# Pass structured fields via extra={"meta": {...}}
logger.info("order created", extra={"meta": {"order_id": "o_1"}})
```

The handler respects the client's `console` setting — do **not** also add a `StreamHandler` or every line will print twice.

Python log levels map as follows:

| Python level         | Ingestor level |
| -------------------- | -------------- |
| `DEBUG`              | `DEBUG`        |
| `INFO`               | `INFO`         |
| `WARNING`            | `WARN`         |
| `ERROR` / `CRITICAL` | `ERROR`        |

## Graceful shutdown

Call `client.close()` before your process exits to flush any buffered entries:

```python
# Flask (SIGTERM handler)
import signal, sys
signal.signal(signal.SIGTERM, lambda *_: (client.close(), sys.exit(0)))

# FastAPI (lifespan)
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app):
    yield
    client.close()

app = FastAPI(lifespan=lifespan)
```

## Direct ingestion

Bypass the internal queue to send entries immediately:

```python
response = client.ingest([
    {
        "projectId": "...",
        "level": "INFO",
        "message": "manual entry",
        "source": "script",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "tags": [],
        "meta": {},
    }
])
# {"ingested": 1, "ids": ["..."]}
```
