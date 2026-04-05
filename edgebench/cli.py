"""
edgebench.cli
~~~~~~~~~~~~~
CLI entry points.

Commands:
  plan              Generate and print an execution plan.
  run               Execute a config file end-to-end.
  summarize         Print stats from a completed run's JSONL.
  report            Generate HTML report from a summary JSON.
  compare           Diff two summary JSON files.
  validate-config   Hard-fail validation of a config file.
"""
from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import os
import sys
import uuid
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_config(path: str):
    from .models import (
        AuthMode, CacheBustPolicy, ConnectionMode, HttpMethod,
        IdentitySpec, ProxySpec, RetryPolicy, RunConfig, RunMode,
        TargetSpec, TransportBackend,
    )
    with open(path) as f:
        raw = json.load(f)

    targets = [
        TargetSpec(
            id=t["id"],
            url=t["url"],
            method=HttpMethod(t.get("method", "PUT")),
            headers=t.get("headers", {}),
            body=t.get("body"),
            auth_mode=AuthMode(t.get("auth_mode", "none")),
            auth_value=t.get("auth_value"),
            expected_status=t.get("expected_status", 200),
            idempotent=t.get("idempotent", True),
        )
        for t in raw.get("targets", [])
    ]
    identities = [
        IdentitySpec(
            profile=i["profile"],
            user_agent=i["user_agent"],
            label=i["label"],
        )
        for i in raw.get("identities", [])
    ]
    proxies = [
        ProxySpec(
            id=p["id"],
            url=p["url"],
            region=p.get("region"),
            provider=p.get("provider"),
        )
        for p in raw.get("proxies", [])
    ]

    cfg = RunConfig(
        run_id=raw.get("run_id") or uuid.uuid4().hex[:12],
        mode=RunMode(raw.get("mode", "benchmark")),
        targets=targets,
        identities=identities,
        proxies=proxies,
        backend=TransportBackend(raw.get("backend", "curl_cffi")),
        connection_mode=ConnectionMode(raw.get("connection_mode", "cold_socket")),
        warmup_runs=raw.get("warmup_runs", 3),
        measured_runs=raw.get("measured_runs", 10),
        inter_request_gap_ms=raw.get("inter_request_gap_ms", 100.0),
        per_group_cooldown_ms=raw.get("per_group_cooldown_ms", 500.0),
        randomize_order=raw.get("randomize_order", False),
        seed=raw.get("seed", 42),
        schedule_start_ns=raw.get("schedule_start_ns"),
        global_concurrency=raw.get("global_concurrency", 10),
        per_host_concurrency=raw.get("per_host_concurrency", 5),
        per_proxy_concurrency=raw.get("per_proxy_concurrency", 3),
        retry_policy=RetryPolicy(raw.get("retry_policy", "disabled")),
        cache_bust_policy=CacheBustPolicy(raw.get("cache_bust_policy", "query_param")),
        connection_reuse=ConnectionMode(raw["connection_reuse"]) if raw.get("connection_reuse") else None,
        output_dir=raw.get("output_dir", "./results"),
        raw_jsonl=raw.get("raw_jsonl", True),
    )
    return cfg


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_validate(args) -> None:
    from .validation import validate, ConfigError
    cfg = _load_config(args.config)
    try:
        validate(cfg)
        print("Config is valid.")
    except ConfigError as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)


def cmd_plan(args) -> None:
    from .planner import build_plan
    from .validation import validate
    cfg = _load_config(args.config)
    validate(cfg)
    plan = build_plan(cfg)
    for spec in plan:
        print(json.dumps({
            "index": spec.request_index,
            "target": spec.target.id,
            "identity": spec.identity.label,
            "proxy": spec.proxy.id if spec.proxy else None,
            "scheduled_ns": spec.scheduled_ns,
            "group_key": spec.group_key,
            "is_warmup": spec.is_warmup,
        }))


def cmd_run(args) -> None:
    from .events import configure_logging
    from .models import EnvironmentCapture, ExperimentReport
    from .planner import build_plan
    from .runner import Runner
    from .validation import validate
    from .writer import write_csv, write_html_report, write_summary_json
    import logging

    configure_logging(logging.INFO if args.quiet else logging.DEBUG)

    cfg = _load_config(args.config)
    validate(cfg)

    os.makedirs(cfg.output_dir, exist_ok=True)
    env = EnvironmentCapture.capture()
    plan = build_plan(cfg)

    runner = Runner(cfg)
    batch = asyncio.run(runner.run(plan))

    report = ExperimentReport(
        run_id=cfg.run_id,
        config=cfg,
        environment=env,
        batch=batch,
        raw_jsonl_path=os.path.join(cfg.output_dir, f"{cfg.run_id}.jsonl"),
    )

    summary_path = os.path.join(cfg.output_dir, f"{cfg.run_id}_summary.json")
    csv_path = os.path.join(cfg.output_dir, f"{cfg.run_id}.csv")
    html_path = os.path.join(cfg.output_dir, f"{cfg.run_id}.html")

    write_summary_json(report, summary_path)
    write_csv(batch.results, csv_path)
    write_html_report(report, html_path)

    report.summary_json_path = summary_path
    report.csv_path = csv_path
    report.html_report_path = html_path

    success = sum(1 for r in batch.results if r.success and not r.is_warmup)
    total = sum(1 for r in batch.results if not r.is_warmup)
    print(f"\nRun complete: {cfg.run_id}")
    print(f"  requests : {total}")
    print(f"  succeeded: {success}")
    print(f"  aborted  : {batch.aborted}")
    print(f"  summary  : {summary_path}")
    print(f"  csv      : {csv_path}")
    print(f"  html     : {html_path}")


def cmd_summarize(args) -> None:
    from .metrics import aggregate
    from .models import RequestResult, TimingBreakdown, FailureKind
    results = []
    with open(args.jsonl) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            d = json.loads(line)
            t = d.get("timing", {})
            timing = TimingBreakdown(
                scheduled_ns=t.get("scheduled_ns", 0),
                awakened_ns=t.get("awakened_ns", 0),
                fired_ns=t.get("fired_ns", 0),
                drift_ns=t.get("drift_ns", 0),
                total_wall_ms=t.get("total_wall_ms"),
                scheduler_drift_ms=t.get("scheduler_drift_ms"),
                client_creation_ms=t.get("client_creation_ms"),
            )
            fk_raw = d.get("failure_kind")
            fk = FailureKind(fk_raw) if fk_raw else None
            r = RequestResult(
                request_index=d.get("request_index", 0),
                group_key=d.get("group_key", ""),
                is_warmup=d.get("is_warmup", False),
                success=d.get("success", False),
                status_code=d.get("status_code"),
                failure_kind=fk,
                timing=timing,
                identity_label=d.get("identity_label", ""),
                proxy_id=d.get("proxy_id"),
                target_id=d.get("target_id", ""),
            )
            results.append(r)

    stats = aggregate(results)
    print(json.dumps({k: dataclasses.asdict(v) for k, v in stats.items()}, indent=2))


def cmd_compare(args) -> None:
    from .compare import compare
    from .models import BatchResult, EnvironmentCapture, ExperimentReport, GroupStats

    def _load_report(path: str) -> ExperimentReport:
        with open(path) as f:
            d = json.load(f)
        gs = {}
        for k, v in d.get("group_stats", {}).items():
            s = GroupStats(**{fk: fv for fk, fv in v.items() if fk in GroupStats.__dataclass_fields__})
            gs[k] = s
        env_d = d.get("environment") or {}
        env = EnvironmentCapture(**{k: v for k, v in env_d.items() if k in EnvironmentCapture.__dataclass_fields__}) if env_d else None
        batch = BatchResult(run_id=d.get("run_id", ""), group_stats=gs)
        return ExperimentReport(run_id=d.get("run_id", ""), environment=env, batch=batch)

    baseline = _load_report(args.baseline)
    candidate = _load_report(args.candidate)
    result = compare(baseline, candidate)

    print(f"Baseline : {result.baseline_run_id}")
    print(f"Candidate: {result.candidate_run_id}")
    print(f"Summary  : {result.summary}")
    print()
    for gk, diff in result.group_diffs.items():
        reg = "⚠ REGRESSION" if diff.regression else "ok"
        print(f"  {gk} [{reg}]")
        if diff.p99_delta_ms is not None:
            print(f"    p99: {diff.p99_delta_ms:+.2f}ms  p95: {diff.p95_delta_ms:+.2f}ms  mean: {diff.mean_delta_ms:+.2f}ms")
        if diff.regression_reasons:
            for r in diff.regression_reasons:
                print(f"    → {r}")
    if result.env_diffs:
        print("\nEnvironment changes:")
        for k, v in result.env_diffs.items():
            print(f"  {k}: {v}")


# ---------------------------------------------------------------------------
# Argparse wiring
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(prog="edgebench", description="Reproducible HTTP edge benchmarking")
    sub = parser.add_subparsers(dest="command", required=True)

    p_validate = sub.add_parser("validate-config", help="Hard-fail validation of a config file")
    p_validate.add_argument("config")
    p_validate.set_defaults(func=cmd_validate)

    p_plan = sub.add_parser("plan", help="Print execution plan as JSONL")
    p_plan.add_argument("config")
    p_plan.set_defaults(func=cmd_plan)

    p_run = sub.add_parser("run", help="Execute a config end-to-end")
    p_run.add_argument("config")
    p_run.add_argument("--quiet", action="store_true")
    p_run.set_defaults(func=cmd_run)

    p_sum = sub.add_parser("summarize", help="Print stats from a raw JSONL file")
    p_sum.add_argument("jsonl")
    p_sum.set_defaults(func=cmd_summarize)

    p_cmp = sub.add_parser("compare", help="Diff two summary JSON files")
    p_cmp.add_argument("baseline")
    p_cmp.add_argument("candidate")
    p_cmp.set_defaults(func=cmd_compare)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
