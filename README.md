# streamlogia — Python SDK

Thread-safe Python client for the Streamlogia log ingestion service. Batches and ships log entries in the background while optionally mirroring them to the console. Requires Python 3.8+ and no third-party dependencies.

## Installation

```bash
pip install streamlogia
```

## Quick start

Set the environment variables once — the SDK picks them up automatically:

```bash
export STREAMLOGIA_API_KEY=your-api-key
export STREAMLOGIA_PROJECT_ID=your-project-id
```

### FastAPI

```python
import streamlogia
from fastapi import FastAPI

app = FastAPI()
client = streamlogia.init(app, source="order-service")
```

### Flask

```python
import streamlogia
from flask import Flask

app = Flask(__name__)
client = streamlogia.init(app, source="payment-service")
```

`init()` does everything in one call: reads credentials from the environment, attaches request-logging middleware, wires the stdlib root logger to the ingestor, and registers a shutdown hook to flush buffered logs on exit.

Use the returned `client` for direct log calls, or use `logging.getLogger(__name__)` anywhere in your app — both route to the ingestor.

```python
import logging

logger = logging.getLogger("order-service")
logger.info("server started")                                    # goes to ingestor
client.info("order created", meta={"order_id": "o_1"})          # also goes to ingestor
```

## `init()` options

| Parameter        | Type       | Default          | Description                                                     |
| ---------------- | ---------- | ---------------- | --------------------------------------------------------------- |
| `app`            | app inst.  | `None`           | Flask or FastAPI/Starlette app. Omit for framework-less usage   |
| `source`         | `str`      | `"unknown"`      | Default source tag applied to every entry                       |
| `api_key`        | `str`      | env var          | Override `STREAMLOGIA_API_KEY`                                  |
| `project_id`     | `str`      | env var          | Override `STREAMLOGIA_PROJECT_ID`                               |
| `batch_size`     | `int`      | `1`              | Flush the queue when it reaches this many entries               |
| `flush_interval` | `float`    | `5.0`            | Background flush interval in seconds                            |
| `console`        | `bool`     | `True`           | Mirror logs to stdout/stderr in addition to the ingestor        |
| `log_level`      | `int`      | `logging.DEBUG`  | Minimum stdlib log level forwarded to the ingestor              |
| `on_error`       | `callable` | prints to stderr | Called with the exception when an ingest request fails          |

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

Pass `console=False` to `init()` if you only want logs in the ingestor and nothing on the terminal.

## Framework integrations

### Request logging middleware

`init()` automatically attaches middleware when you pass an app. Each HTTP request produces one log entry after the response is sent, containing:

`method`, `path`, `status`, `duration_ms`, `ip`, `user_agent`, `request_id` (when `X-Request-Id` / `x-request-id` header is present)

Status codes ≥ 500 → `ERROR`, ≥ 400 → `WARN`, everything else → `INFO`.

See [`examples/fastapi_app.py`](examples/fastapi_app.py) and [`examples/flask_app.py`](examples/flask_app.py) for complete examples.

### stdlib `logging` integration

`init()` wires the stdlib root logger automatically. You can also attach a handler manually to a specific logger:

```python
import logging

logger = logging.getLogger("myapp")
logger.setLevel(logging.DEBUG)
logger.addHandler(client.logging_handler())

logger.info("server started")
logger.error("unhandled exception", exc_info=True)  # traceback captured in meta

# Pass structured fields via extra={"meta": {...}}
logger.info("order created", extra={"meta": {"order_id": "o_1"}})
```

Do **not** add a separate `StreamHandler` — the client's `console=True` setting already handles stdout/stderr output and a second handler would print every line twice.

Python log levels map as follows:

| Python level         | Ingestor level |
| -------------------- | -------------- |
| `DEBUG`              | `DEBUG`        |
| `INFO`               | `INFO`         |
| `WARNING`            | `WARN`         |
| `ERROR` / `CRITICAL` | `ERROR`        |

## Advanced: manual client setup

If you need full control you can create and manage the client directly:

```python
from streamlogia import LogIngestorClient

client = LogIngestorClient(
    api_key="...",       # or omit to read STREAMLOGIA_API_KEY
    project_id="...",    # or omit to read STREAMLOGIA_PROJECT_ID
    source="worker",
    batch_size=50,
    console=False,
)

client.info("job started")
client.close()  # flush before exit
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

## Migration guide

### From `logingestor` to `streamlogia`

Version 0.3.0 renames the package module and environment variables. The following changes are required:

**Imports**

```python
# Before
import logingestor
client = logingestor.init(app, source="my-service")

# After
import streamlogia
client = streamlogia.init(app, source="my-service")
```

**Manual client setup**

```python
# Before
from logingestor import LogIngestorClient

# After
from streamlogia import LogIngestorClient
```

**Environment variables**

```bash
# Before
export LOGINGESTOR_API_KEY=your-api-key
export LOGINGESTOR_PROJECT_ID=your-project-id

# After
export STREAMLOGIA_API_KEY=your-api-key
export STREAMLOGIA_PROJECT_ID=your-project-id
```
