"""Benchmark each collector alone at increasing trace rates, replaying one recorded capture.

For every rate, each collector runs by itself in Docker (so memory and CPU are its own) and
receives the same seeded replay (`mock-io-replay`). The baseline_naive run of that rate is the
reference the other outputs are scored against. Per run it reports the achieved rate, refused
requests, peak container memory, whether Docker killed the collector (OOM), the share of traces
kept and recall by trace type.

Usage:
  uv run python scripts/benchmark_rates.py \\
    --capture captures/supermarket_seed42/baseline_naive.jsonl \\
    --ground-truth captures/supermarket_seed42/ground_truth.jsonl --rates 5,10,20,40
"""

import argparse
import contextlib
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

os.environ.setdefault("GRPC_VERBOSITY", "ERROR")  # silence fork warnings from docker calls

import yaml

from mock_io_app.evaluation import (
    evaluate,
    label_traces,
    load_anomaly_spans,
    load_ground_truth,
    load_traces,
)
from mock_io_app.replay import grpc_exporter, load_capture, replay

ROOT = Path(__file__).resolve().parent.parent
CONTAINER = "otelcol-bench"
IMAGE = "otel/opentelemetry-collector-contrib:latest"
REFERENCE = "baseline_naive"
DEFAULT_COLLECTORS = [
    "baseline_naive",
    "sampling",
    "anomalies_sampling",
    "anomalies_sampling_limited",
    "span_filter",
    "routed_tail_sampling",
]
UNITS = {"B": 1, "KIB": 2**10, "MIB": 2**20, "GIB": 2**30, "KB": 1e3, "MB": 1e6, "GB": 1e9}


def parse_seconds(text: str) -> float:
    match = re.fullmatch(r"(\d+(?:\.\d+)?)(ms|s|m)", text)
    if not match:
        raise argparse.ArgumentTypeError(f"expected a duration like 5s, got {text!r}")
    return float(match[1]) * {"ms": 0.001, "s": 1, "m": 60}[match[2]]


def parse_mem_usage(text: str) -> float:
    """Bytes in use from `docker stats` MemUsage, e.g. '123.4MiB / 7.6GiB'."""
    match = re.match(r"\s*([\d.]+)\s*([A-Za-z]+)", text)
    if not match or match[2].upper() not in UNITS:
        raise ValueError(f"cannot parse memory usage {text!r}")
    return float(match[1]) * UNITS[match[2].upper()]


def hold_seconds(name: str, decision_wait: str) -> float:
    """Longest tail-sampling hold of a collector: --decision-wait, or fixed ones in its config."""
    config = yaml.safe_load((ROOT / "collectors" / name / "config.yaml").read_text())
    waits = [
        parse_seconds(settings["decision_wait"])
        for settings in (config.get("processors") or {}).values()
        if isinstance(settings, dict)
        and "decision_wait" in settings
        and "${" not in str(settings["decision_wait"])
    ]
    return max([parse_seconds(decision_wait), *waits])


def docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["docker", *args], capture_output=True, text=True, check=check)


class CollectorContainer:
    """One collector in Docker, writing its output to out_dir; tracks peak memory."""

    def __init__(self, name: str, out_dir: Path, decision_wait: str, memory: str | None) -> None:
        self.name = name
        self.out_dir = out_dir
        self.decision_wait = decision_wait
        self.memory = memory
        self.peak_bytes = 0.0
        self._stop_sampling = threading.Event()
        self._sampler: threading.Thread | None = None

    def start(self) -> None:
        docker("rm", "-f", CONTAINER, check=False)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        config = ROOT / "collectors" / self.name / "config.yaml"
        docker(
            "run", "-d", "--name", CONTAINER,
            "--user", f"{os.getuid()}:{os.getgid()}",
            "-e", f"DECISION_WAIT={self.decision_wait}",
            *(["--memory", self.memory] if self.memory else []),
            "-p", "4317:4317",
            "-v", f"{self.out_dir.resolve()}:/out",
            "-v", f"{config}:/etc/otelcol/{self.name}.yaml:ro",
            IMAGE, f"--config=/etc/otelcol/{self.name}.yaml",
        )  # fmt: skip
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            logs = docker("logs", CONTAINER, check=False)
            if "Everything is ready" in logs.stdout + logs.stderr:
                break
            if docker("inspect", "-f", "{{.State.Running}}", CONTAINER).stdout.strip() != "true":
                raise RuntimeError(f"{self.name} exited at startup:\n{logs.stderr[-2000:]}")
            time.sleep(0.5)
        else:
            raise RuntimeError(f"{self.name} not ready after 60s")
        self._sampler = threading.Thread(target=self._sample, daemon=True)
        self._sampler.start()

    def _sample(self) -> None:
        while not self._stop_sampling.is_set():
            result = docker(
                "stats", "--no-stream", "--format", "{{.MemUsage}}", CONTAINER, check=False
            )
            # Parsing fails once the container has stopped.
            with contextlib.suppress(ValueError):
                self.peak_bytes = max(self.peak_bytes, parse_mem_usage(result.stdout))

    def stop(self) -> dict[str, Any]:
        """Stop (SIGTERM flushes batches), return the exit state and remove the container."""
        self._stop_sampling.set()
        docker("stop", "-t", "60", CONTAINER, check=False)
        state = docker("inspect", "-f", "{{.State.OOMKilled}} {{.State.ExitCode}}", CONTAINER)
        oom, code = state.stdout.split()
        docker("rm", "-f", CONTAINER, check=False)
        if self._sampler:
            self._sampler.join(timeout=10)
        return {"oom_killed": oom == "true", "exit_code": int(code)}


def run_collector(
    name: str, run_dir: Path, templates: list, args: argparse.Namespace, rate: float, traces: int
) -> dict[str, Any]:
    container = CollectorContainer(name, run_dir, args.decision_wait, args.memory)
    container.start()
    truth = None
    try:
        if name == REFERENCE:
            truth = (run_dir / "ground_truth.jsonl").open("w")
        stats = replay(
            templates,
            grpc_exporter(args.endpoint),
            traces=traces,
            rate=rate,
            workers=args.workers,
            batch=args.batch,
            seed=args.seed,
            ground_truth_out=truth,
        )
        # Let tail sampling decide the last traces and the exporter write them.
        time.sleep(hold_seconds(name, args.decision_wait) + 3)
    finally:
        if truth:
            truth.close()
        state = container.stop()
    return {
        "collector": name,
        "rate": rate,
        "traces_sent": stats.traces,
        "achieved_traces_per_s": stats.traces / stats.elapsed if stats.elapsed else None,
        "failed_requests": stats.failed_requests,
        "errors": dict(stats.errors),
        "failed_traces": round(stats.failed_spans / max(stats.spans, 1) * stats.traces),
        "peak_memory_mb": container.peak_bytes / 1e6,
        **state,
    }


def score_rate(runs: list[dict[str, Any]], rate_dir: Path) -> None:
    """Add reduction and recall to each run, scoring against this rate's baseline output."""
    reference_path = rate_dir / REFERENCE / f"{REFERENCE}.jsonl"
    reference = load_traces(reference_path)
    truth_path = rate_dir / REFERENCE / "ground_truth.jsonl"
    labels = label_traces(reference, load_ground_truth(truth_path))
    anomaly_spans = load_anomaly_spans(truth_path)
    for run in runs:
        path = rate_dir / run["collector"] / f"{run['collector']}.jsonl"
        output = load_traces(path) if path.exists() else {}
        report = evaluate(
            reference,
            output,
            labels,
            reference_path.stat().st_size,
            path.stat().st_size if path.exists() else 0,
            anomaly_spans,
        )
        run["reference_traces"] = len(reference)
        run["traces_kept"] = report["reduction"]["traces_kept"]
        run["spans_kept"] = report["reduction"]["spans_kept"]
        run["bytes_kept"] = report["reduction"]["bytes_kept"]
        run["evidence"] = report["evidence"]["recall"]
        run["recall"] = {name: row["recall"] for name, row in report["recall"].items()}


def _pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.1%}"


def format_table(runs: list[dict[str, Any]]) -> str:
    headers = [
        "rate",
        "collector",
        "achieved/s",
        "refused",
        "peak MB",
        "OOM",
        "kept",
        "spans",
        "evidence",
        "anomalous",
        "error",
        "slow",
        "normal",
    ]
    rows = []
    for run in runs:
        recall = run.get("recall", {})
        rows.append(
            [
                f"{run['rate']:g}",
                run["collector"],
                f"{run['achieved_traces_per_s']:.1f}",
                str(run["failed_requests"])
                + (" " + ",".join(run["errors"]) if run["errors"] else ""),
                f"{run['peak_memory_mb']:.0f}",
                "yes" if run["oom_killed"] else "no",
                _pct(run.get("traces_kept")),
                _pct(run.get("spans_kept")),
                _pct(run.get("evidence")),
                *(_pct(recall.get(t)) for t in ("anomalous", "error", "slow", "normal")),
            ]
        )
    widths = [max(len(h), *(len(r[i]) for r in rows)) for i, h in enumerate(headers)]
    lines = [
        "  ".join(
            h.ljust(w) if i < 2 else h.rjust(w)
            for i, (h, w) in enumerate(zip(headers, widths, strict=True))
        )
    ]
    for row in rows:
        lines.append(
            "  ".join(
                c.ljust(w) if i < 2 else c.rjust(w)
                for i, (c, w) in enumerate(zip(row, widths, strict=True))
            )
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--ground-truth", type=Path, required=True)
    parser.add_argument("--rates", default="5,10,20,40", help="comma-separated traces/s")
    parser.add_argument("--collectors", nargs="+", default=DEFAULT_COLLECTORS)
    parser.add_argument(
        "--duration",
        type=float,
        default=30,
        help="seconds of load per run (traces = rate x duration)",
    )
    parser.add_argument("--traces", type=int, help="fixed traces per run instead of --duration")
    parser.add_argument(
        "--decision-wait",
        default="5s",
        help="tail sampling decision_wait (replayed traces arrive whole)",
    )
    parser.add_argument(
        "--memory", help="docker memory limit per collector, e.g. 512m (OOM at lower load)"
    )
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--batch", type=int, default=5)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--endpoint", default="http://127.0.0.1:4317")
    parser.add_argument("--out-dir", type=Path, default=ROOT / "bench")
    parser.add_argument("--keep", action="store_true", help="keep collector outputs (large)")
    args = parser.parse_args(argv)

    parse_seconds(args.decision_wait)
    rates = [float(r) for r in args.rates.split(",")]
    collectors = [REFERENCE] + [c for c in args.collectors if c != REFERENCE]
    for name in collectors:
        if not (ROOT / "collectors" / name / "config.yaml").exists():
            parser.error(f"unknown collector: {name}")

    templates = load_capture(args.capture, args.ground_truth)
    print(f"loaded {len(templates)} traces from {args.capture}", flush=True)
    all_runs: list[dict[str, Any]] = []
    for rate in rates:
        traces = args.traces or max(1, round(rate * args.duration))
        rate_dir = args.out_dir / f"rate_{rate:g}"
        shutil.rmtree(rate_dir, ignore_errors=True)
        runs = []
        for name in collectors:
            print(f"rate {rate:g}/s: {name} ({traces} traces)", flush=True)
            runs.append(run_collector(name, rate_dir / name, templates, args, rate, traces))
        score_rate(runs, rate_dir)
        all_runs += runs
        if not args.keep:
            shutil.rmtree(rate_dir, ignore_errors=True)
        print(format_table(all_runs), flush=True)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    (args.out_dir / "results.json").write_text(json.dumps(all_runs, indent=2))
    print(f"\nwrote {args.out_dir / 'results.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
