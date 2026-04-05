"""
edgebench.writer
~~~~~~~~~~~~~~~~
Append-only JSONL writer during runs (crash-safe).
Post-run: summary JSON, CSV, HTML report.
"""
from __future__ import annotations

import csv
import dataclasses
import json
import os
import time
from pathlib import Path
from typing import Dict, List, Optional

from .models import (
    BatchResult,
    EnvironmentCapture,
    ExperimentReport,
    GroupStats,
    RequestResult,
    RunConfig,
    RESULT_SCHEMA_VERSION,
    REPORT_SCHEMA_VERSION,
)


class ResultWriter:
    """Writes raw results to JSONL during the run. One line per request."""

    def __init__(self, path: str) -> None:
        self._path = path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._fh = open(path, "a", buffering=1)  # line-buffered

    def write(self, result: RequestResult) -> None:
        self._fh.write(json.dumps(dataclasses.asdict(result)) + "\n")

    def flush(self) -> None:
        self._fh.flush()

    def close(self) -> None:
        self._fh.flush()
        self._fh.close()


def write_summary_json(report: ExperimentReport, path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    data = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "run_id": report.run_id,
        "environment": dataclasses.asdict(report.environment) if report.environment else None,
        "group_stats": {
            k: dataclasses.asdict(v)
            for k, v in (report.batch.group_stats.items() if report.batch else {})
        },
        "aborted": report.batch.aborted if report.batch else False,
    }
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def write_csv(results: List[RequestResult], path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if not results:
        return
    fields = [
        "request_index", "group_key", "is_warmup", "success", "status_code",
        "failure_kind", "identity_label", "proxy_id", "target_id",
        "backend", "connection_mode",
        "scheduled_ns", "fired_ns", "drift_ns", "scheduler_drift_ms",
        "total_wall_ms", "client_creation_ms", "connect_ms", "tls_ms",
        "first_byte_ms",
    ]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for r in results:
            row = {
                "request_index": r.request_index,
                "group_key": r.group_key,
                "is_warmup": r.is_warmup,
                "success": r.success,
                "status_code": r.status_code,
                "failure_kind": r.failure_kind.value if r.failure_kind else "",
                "identity_label": r.identity_label,
                "proxy_id": r.proxy_id or "",
                "target_id": r.target_id,
                "backend": r.backend,
                "connection_mode": r.connection_mode,
                "scheduled_ns": r.timing.scheduled_ns,
                "fired_ns": r.timing.fired_ns,
                "drift_ns": r.timing.drift_ns,
                "scheduler_drift_ms": r.timing.scheduler_drift_ms,
                "total_wall_ms": r.timing.total_wall_ms,
                "client_creation_ms": r.timing.client_creation_ms,
                "connect_ms": r.timing.connect_ms,
                "tls_ms": r.timing.tls_ms,
                "first_byte_ms": r.timing.first_byte_ms,
            }
            writer.writerow(row)


def write_html_report(report: ExperimentReport, path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    batch = report.batch
    env = report.environment
    groups = batch.group_stats if batch else {}

    rows = ""
    for gk, s in groups.items():
        rows += f"""
        <tr>
          <td>{gk}</td>
          <td>{s.count}</td>
          <td>{s.success_rate*100:.1f}%</td>
          <td>{_fmt(s.min_ms)}</td>
          <td>{_fmt(s.mean_ms)}</td>
          <td>{_fmt(s.median_ms)}</td>
          <td>{_fmt(s.p90_ms)}</td>
          <td>{_fmt(s.p95_ms)}</td>
          <td>{_fmt(s.p99_ms)}</td>
          <td>{_fmt(s.max_ms)}</td>
          <td>{_fmt(s.stddev_ms)}</td>
          <td>{s.outlier_count}</td>
          <td>{_fmt_drift(s.drift_mean_ns)}</td>
          <td>{_err_str(s.error_distribution)}</td>
        </tr>"""

    env_rows = ""
    if env:
        for k, v in dataclasses.asdict(env).items():
            env_rows += f"<tr><td>{k}</td><td>{v}</td></tr>"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>edgebench — {report.run_id}</title>
<style>
  body {{ font-family: monospace; background: #0d1117; color: #c9d1d9; padding: 2rem; }}
  h1, h2 {{ color: #58a6ff; }}
  table {{ border-collapse: collapse; width: 100%; margin-bottom: 2rem; }}
  th {{ background: #161b22; color: #8b949e; text-align: left; padding: 6px 10px; }}
  td {{ padding: 5px 10px; border-bottom: 1px solid #21262d; }}
  tr:hover td {{ background: #161b22; }}
  .aborted {{ color: #f85149; }}
</style>
</head>
<body>
<h1>edgebench run: {report.run_id}</h1>
{"<p class='aborted'>⚠ Run was ABORTED</p>" if (batch and batch.aborted) else ""}

<h2>Results by Group</h2>
<table>
<thead><tr>
  <th>Group</th><th>N</th><th>Success</th>
  <th>Min</th><th>Mean</th><th>Median</th>
  <th>P90</th><th>P95</th><th>P99</th><th>Max</th>
  <th>StdDev</th><th>Outliers</th><th>Drift mean</th><th>Errors</th>
</tr></thead>
<tbody>{rows}</tbody>
</table>

<h2>Environment</h2>
<table><thead><tr><th>Key</th><th>Value</th></tr></thead>
<tbody>{env_rows}</tbody></table>
</body></html>"""

    with open(path, "w") as f:
        f.write(html)


def _fmt(v: Optional[float]) -> str:
    return f"{v:.3f} ms" if v is not None else "—"


def _fmt_drift(v: Optional[float]) -> str:
    return f"{v/1000:.1f} µs" if v is not None else "—"


def _err_str(d: Dict[str, int]) -> str:
    if not d:
        return "none"
    return " ".join(f"{k}:{v}" for k, v in d.items())
