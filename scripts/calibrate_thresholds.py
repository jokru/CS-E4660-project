"""Calibrate latency thresholds from a baseline capture and generate the collectors that use them.

Writes:
- a thresholds JSON (per (service.name, span name) span thresholds, per run job statistics),
  read by the app-side reducers through MOCK_IO_THRESHOLDS;
- collectors/span_filter/config.yaml: drops fast, non-error detail spans (stateless);
- collectors/routed_tail_sampling/config.yaml: routes spans by `mock_io.run_job` to tail samplers
  whose decision_wait fits that job, with a per-job latency policy.

A span threshold is `factor` x the median duration of its key (the median is barely moved by
injected anomalies, unlike a high percentile). Keys with fewer than `min_samples` spans get none.
Calibrate on a different seed than the one you evaluate on.

Usage:
  uv run python scripts/calibrate_thresholds.py \\
    --capture captures/supermarket_seed42/baseline_naive.jsonl --out thresholds/supermarket.json
"""

import argparse
import json
import math
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parent.parent
SKELETON_PREFIXES = ("run.", "job.", "op.")
BUCKETS_S = (15, 30, 45, 60, 90, 120, 180, 300, 600, 900)
RUN_JOB_ATTRIBUTE = "mock_io.run_job"


def read_spans(path: Path) -> tuple[dict[tuple[str, str], list[int]], dict[str, list[int]]]:
    """Durations (ns) per (service.name, span name), and per job from its `run.<job>` spans."""
    by_key: dict[tuple[str, str], list[int]] = defaultdict(list)
    runs: dict[str, list[int]] = defaultdict(list)
    with path.open() as handle:
        for line in handle:
            if not line.strip():
                continue
            for resource_spans in json.loads(line).get("resourceSpans", []):
                service = next(
                    (
                        a["value"].get("stringValue", "")
                        for a in resource_spans.get("resource", {}).get("attributes", [])
                        if a["key"] == "service.name"
                    ),
                    "",
                )
                for scope_spans in resource_spans.get("scopeSpans", []):
                    for span in scope_spans.get("spans", []):
                        duration = int(span["endTimeUnixNano"]) - int(span["startTimeUnixNano"])
                        by_key[(service, span["name"])].append(duration)
                        if span["name"].startswith("run."):
                            runs[span["name"].removeprefix("run.")].append(duration)
    return by_key, runs


def bucket_for(seconds: float) -> int:
    return next((b for b in BUCKETS_S if b >= seconds), BUCKETS_S[-1])


def calibrate(
    by_key: dict[tuple[str, str], list[int]],
    runs: dict[str, list[int]],
    *,
    factor: float,
    min_samples: int,
    job_factor: float,
    wait_margin: float,
) -> dict[str, Any]:
    spans = []
    for (service, name), durations in sorted(by_key.items()):
        if name.startswith(SKELETON_PREFIXES) or len(durations) < min_samples:
            continue
        median_ms = statistics.median(durations) / 1e6
        spans.append(
            {
                "service": service,
                "name": name,
                "samples": len(durations),
                "p50_ms": round(median_ms, 3),
                "threshold_ms": round(median_ms * factor, 3),
            }
        )
    jobs = []
    for job, durations in sorted(runs.items()):
        median_s = statistics.median(durations) / 1e9
        max_s = max(durations) / 1e9
        jobs.append(
            {
                "job": job,
                "runs": len(durations),
                "p50_s": round(median_s, 3),
                "max_s": round(max_s, 3),
                "latency_threshold_ms": math.ceil(median_s * job_factor * 1000),
                # Tail sampling decides decision_wait after a trace's first span arrives.
                "decision_wait_s": bucket_for(max_s * wait_margin + 5),
            }
        )
    return {
        "factor": factor,
        "min_samples": min_samples,
        "job_factor": job_factor,
        "skeleton_prefixes": list(SKELETON_PREFIXES),
        "spans": spans,
        "jobs": jobs,
    }


def _quote(text: str) -> str:
    return json.dumps(text)


def span_filter_config(thresholds: dict[str, Any], source: str, baseline_percent: float) -> str:
    conditions = [
        f'resource.attributes["service.name"] == {_quote(row["service"])}'
        f" and span.name == {_quote(row['name'])}"
        f' and (span.end_time - span.start_time) < Duration("{row["threshold_ms"]}ms")'
        " and span.status.code != STATUS_CODE_ERROR"
        for row in thresholds["spans"]
    ]
    config = {
        "receivers": _otlp_receiver(),
        "processors": {
            "filter/span_filter": {"error_mode": "ignore", "trace_conditions": conditions},
            "probabilistic_sampler/span_filter": {"sampling_percentage": baseline_percent},
            "batch": {},
        },
        "exporters": {"file/span_filter": {"path": "/out/span_filter.jsonl"}},
        "service": {
            "pipelines": {
                "traces/span_filter": {
                    "receivers": ["otlp"],
                    "processors": ["filter/span_filter", "batch"],
                    "exporters": ["file/span_filter"],
                },
                # A few whole traces as a normal-behaviour baseline (kept spans repeat here).
                "traces/span_filter_baseline": {
                    "receivers": ["otlp"],
                    "processors": ["probabilistic_sampler/span_filter", "batch"],
                    "exporters": ["file/span_filter"],
                },
            }
        },
    }
    header = (
        "# span_filter: stateless span-level reduction. Drops detail spans that are faster than\n"
        "# their calibrated threshold and not errors; the skeleton (run./job./op.) and unknown\n"
        f"# spans are kept, plus {baseline_percent:g}% of whole traces. GENERATED by\n"
        f"# scripts/calibrate_thresholds.py from {source}; rerun it instead of editing.\n"
    )
    return header + yaml.safe_dump(config, sort_keys=False, width=1000)


def routed_config(thresholds: dict[str, Any], source: str, baseline_percent: float) -> str:
    name = "routed_tail_sampling"
    by_bucket: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for job in thresholds["jobs"]:
        by_bucket[job["decision_wait_s"]].append(job)
    default_wait = max([120, *by_bucket])

    def pipeline(wait: int) -> str:
        return f"traces/{name}_{wait}s"

    def tail_sampling(wait: int, jobs: list[dict[str, Any]]) -> dict[str, Any]:
        policies: list[dict[str, Any]] = [
            {"name": "errors", "type": "status_code", "status_code": {"status_codes": ["ERROR"]}}
        ]
        for job in jobs:
            policies.append(
                {
                    "name": f"slow_{job['job']}",
                    "type": "and",
                    "and": {
                        "and_sub_policy": [
                            {
                                "name": "job",
                                "type": "string_attribute",
                                "string_attribute": {
                                    "key": RUN_JOB_ATTRIBUTE,
                                    "values": [job["job"]],
                                },
                            },
                            {
                                "name": "latency",
                                "type": "latency",
                                "latency": {"threshold_ms": job["latency_threshold_ms"]},
                            },
                        ]
                    },
                }
            )
        if not jobs:
            policies.append({"name": "slow", "type": "latency", "latency": {"threshold_ms": 1000}})
        policies.append(
            {
                "name": "baseline",
                "type": "probabilistic",
                "probabilistic": {"sampling_percentage": baseline_percent},
            }
        )
        return {"decision_wait": f"{wait}s", "num_traces": 100000, "policies": policies}

    processors: dict[str, Any] = {}
    pipelines: dict[str, Any] = {
        f"traces/{name}": {
            "receivers": ["otlp"],
            "processors": [],
            "exporters": [f"routing/{name}"],
        }
    }
    table = []
    for wait in sorted(by_bucket):
        jobs = by_bucket[wait]
        condition = " or ".join(
            f"span.attributes[{_quote(RUN_JOB_ATTRIBUTE)}] == {_quote(j['job'])}" for j in jobs
        )
        table.append({"condition": condition, "pipelines": [pipeline(wait)]})
    for wait in sorted({*by_bucket, default_wait}):
        processors[f"tail_sampling/{name}_{wait}s"] = tail_sampling(wait, by_bucket.get(wait, []))
        pipelines[pipeline(wait)] = {
            "receivers": [f"routing/{name}"],
            "processors": [f"tail_sampling/{name}_{wait}s", "batch"],
            "exporters": [f"file/{name}"],
        }
    processors["batch"] = {}
    config = {
        "receivers": _otlp_receiver(),
        "connectors": {
            f"routing/{name}": {"default_pipelines": [pipeline(default_wait)], "table": table}
        },
        "processors": processors,
        "exporters": {f"file/{name}": {"path": f"/out/{name}.jsonl"}},
        "service": {"pipelines": pipelines},
    }
    header = (
        "# routed_tail_sampling: tail sampling with a hold time per job. Spans are routed by the\n"
        f"# app's {RUN_JOB_ATTRIBUTE} attribute to a tail sampler whose decision_wait fits that\n"
        "# job's longest run; spans without it go to the default. Keeps errors, runs slower than\n"
        f"# their job's threshold, and {baseline_percent:g}% of traces. GENERATED by\n"
        f"# scripts/calibrate_thresholds.py from {source}; rerun it instead of editing.\n"
    )
    return header + yaml.safe_dump(config, sort_keys=False, width=1000)


def _otlp_receiver() -> dict[str, Any]:
    return {
        "otlp": {
            "protocols": {
                "grpc": {"endpoint": "0.0.0.0:4317"},
                "http": {"endpoint": "0.0.0.0:4318"},
            }
        }
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--capture", type=Path, required=True, help="baseline_naive capture")
    parser.add_argument("--out", type=Path, default=ROOT / "thresholds" / "supermarket.json")
    parser.add_argument("--factor", type=float, default=5.0, help="span threshold / median")
    parser.add_argument("--min-samples", type=int, default=20)
    parser.add_argument(
        "--job-factor", type=float, default=1.5, help="per-job latency threshold / median run"
    )
    parser.add_argument(
        "--wait-margin", type=float, default=1.25, help="decision_wait / longest run"
    )
    parser.add_argument("--baseline-percent", type=float, default=1.0)
    parser.add_argument("--collectors-dir", type=Path, default=ROOT / "collectors")
    args = parser.parse_args(argv)

    by_key, runs = read_spans(args.capture)
    thresholds = calibrate(
        by_key,
        runs,
        factor=args.factor,
        min_samples=args.min_samples,
        job_factor=args.job_factor,
        wait_margin=args.wait_margin,
    )
    thresholds["source"] = str(args.capture)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(thresholds, indent=1) + "\n")

    source = args.capture.as_posix()
    for name, text in (
        ("span_filter", span_filter_config(thresholds, source, args.baseline_percent)),
        ("routed_tail_sampling", routed_config(thresholds, source, args.baseline_percent)),
    ):
        path = args.collectors_dir / name / "config.yaml"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        print(f"wrote {path}")
    buckets = sorted({j["decision_wait_s"] for j in thresholds["jobs"]})
    print(
        f"wrote {args.out}: {len(thresholds['spans'])} span thresholds, "
        f"{len(thresholds['jobs'])} jobs in decision_wait buckets {buckets}"
    )
    if not runs:
        print("warning: no run.* spans; routed_tail_sampling only has the default", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
