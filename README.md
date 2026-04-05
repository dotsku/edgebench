# edgebench

Reproducible HTTP edge benchmarking with typed configs, execution planning, structured results, and report generation.

## What It Does

`edgebench` is a benchmarking harness for comparing HTTP request behavior across targets, client identities, and transport settings.

It focuses on reproducibility:

- config is validated before execution
- request plans are generated before the run starts
- structured events are emitted during execution
- raw results are written immediately to disk
- runs can be summarized, compared, and turned into reports

The goal is to make benchmarking less ad hoc and easier to inspect after the fact.

## Features

- Typed config and runtime models
- Deterministic execution planning
- Multiple transport backends (`curl_cffi`, `httpx`)
- Warmup and measured runs
- Concurrency controls (global, per-host, per-proxy)
- Structured JSON event logging
- Raw JSONL result capture
- Summary JSON, CSV, and HTML report output
- Run-to-run comparison with regression detection
- Test coverage for planner, metrics, validation, serialization, RNG, and transport reuse

## Installation

Python 3.11+ is recommended.

```bash
python3.11 -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
pip install -e '.[dev]'
```

## Running Tests

```bash
python -m pytest -q
```

## Quick Start

Validate the demo config:

```bash
edgebench validate-config configs/demo.json
```

Preview the execution plan:

```bash
edgebench plan configs/demo.json | head
```

Run the benchmark:

```bash
edgebench run configs/demo.json
```

After a run, output files are written to `results/`:

- `*.jsonl` raw request results
- `*.csv` tabular export
- `*_summary.json` aggregated summary
- `*.html` HTML report

Compare two summary files:

```bash
edgebench compare results/run_a_summary.json results/run_b_summary.json
```

## Project Layout

```text
edgebench/
├── cli.py          # command-line entry points
├── models.py       # typed models and schemas
├── planner.py      # deterministic request planning
├── runner.py       # orchestration and execution
├── transport.py    # backend abstraction and HTTP execution
├── scheduler.py    # timing and fire scheduling
├── metrics.py      # aggregation and statistics
├── validation.py   # config validation
├── writer.py       # raw and report output
├── compare.py      # run comparison
├── clock.py        # time abstraction
├── rng.py          # seeded randomness
├── events.py       # structured event logging
└── __init__.py     # public API

tests/
└── test_edgebench.py

configs/
└── demo.json
```

## Methodology

What is measured:

- `total_wall_ms`: time spent around request execution
- `scheduler_drift_ms`: difference between scheduled and actual fire time
- `client_creation_ms`: time to construct the transport session/client

What is excluded or environment-dependent:

- warmup runs are excluded from aggregated statistics
- some lower-level timing details depend on backend capabilities
- scheduling precision depends on local machine load and runtime behavior

Reproducibility:

- all randomness flows through a seeded RNG
- the execution plan is determined from config before the run begins
- same config plus same seed produces the same request plan

## Requirements

- Python 3.11+
- `curl_cffi` for impersonation-oriented transport experiments
- `httpx` as a simpler async backend

## Disclaimer

Use only against systems and endpoints you own or are explicitly authorized to test.

## License

MIT
