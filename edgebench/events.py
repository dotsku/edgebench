"""
edgebench.events
~~~~~~~~~~~~~~~~
Structured event emission.  All log lines are JSON, versioned, and typed.
No ad-hoc logger.debug() calls in execution path.
"""
from __future__ import annotations

import json
import logging
import sys
import time
from dataclasses import asdict
from typing import Any, Dict, Optional

from .models import Event, EventKind, EVENT_SCHEMA_VERSION

# ---------------------------------------------------------------------------
# JSON formatter
# ---------------------------------------------------------------------------

class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base: Dict[str, Any] = {
            "ts_iso": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "msg": record.getMessage(),
            "logger": record.name,
        }
        # Merge any extra= fields set by the caller
        skip = {
            "msg", "args", "levelname", "levelno", "pathname", "filename",
            "module", "exc_info", "exc_text", "stack_info", "lineno",
            "funcName", "created", "msecs", "relativeCreated", "thread",
            "threadName", "processName", "process", "name", "message",
            "taskName",
        }
        for k, v in record.__dict__.items():
            if k not in skip:
                base[k] = v
        if record.exc_info:
            base["exc"] = self.formatException(record.exc_info)
        return json.dumps(base)


def configure_logging(level: int = logging.DEBUG) -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(_JsonFormatter())
    root = logging.getLogger("edgebench")
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
    root.propagate = False


logger = logging.getLogger("edgebench")

# ---------------------------------------------------------------------------
# Typed event emitter
# ---------------------------------------------------------------------------

def emit(
    kind: EventKind,
    run_id: str,
    request_index: Optional[int] = None,
    **payload: Any,
) -> None:
    event = Event(
        schema_version=EVENT_SCHEMA_VERSION,
        kind=kind,
        run_id=run_id,
        request_index=request_index,
        payload=payload,
        ts_ns=time.monotonic_ns(),
    )
    level = logging.ERROR if kind == EventKind.REQUEST_FAILED else logging.DEBUG
    logger.log(
        level,
        kind.value,
        extra={
            "schema_version": event.schema_version,
            "event_kind": kind.value,
            "run_id": run_id,
            "request_index": request_index,
            **payload,
        },
    )


def emit_run_started(run_id: str, config_summary: Dict[str, Any]) -> None:
    emit(EventKind.RUN_STARTED, run_id, config=config_summary)


def emit_request_scheduled(run_id: str, idx: int, scheduled_ns: int, group_key: str) -> None:
    emit(EventKind.REQUEST_SCHEDULED, run_id, idx, scheduled_ns=scheduled_ns, group_key=group_key)


def emit_request_fired(run_id: str, idx: int, fired_ns: int, drift_ns: int) -> None:
    emit(EventKind.REQUEST_FIRED, run_id, idx, fired_ns=fired_ns, drift_ns=drift_ns)


def emit_response_received(run_id: str, idx: int, status_code: int, total_wall_ms: float) -> None:
    emit(EventKind.RESPONSE_RECEIVED, run_id, idx, status_code=status_code, total_wall_ms=total_wall_ms)


def emit_request_failed(run_id: str, idx: int, failure_kind: str, detail: str) -> None:
    emit(EventKind.REQUEST_FAILED, run_id, idx, failure_kind=failure_kind, detail=detail)


def emit_run_completed(run_id: str, total_requests: int, success_count: int) -> None:
    emit(EventKind.RUN_COMPLETED, run_id, total_requests=total_requests, success_count=success_count)


def emit_run_aborted(run_id: str, reason: str) -> None:
    emit(EventKind.RUN_ABORTED, run_id, reason=reason)
