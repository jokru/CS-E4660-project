"""Score a collector's output against a reference (baseline) capture and the ground-truth log.

Inputs are OTLP JSON lines as written by the OpenTelemetry Collector `file` exporter, and the
JSONL written through MOCK_IO_GROUND_TRUTH_FILE. The reference capture is the "keep everything"
baseline for the same workload.
"""

import argparse
import json
import math
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

SLOW_PERCENTILE = 95.0


@dataclass
class TraceInfo:
    spans: set[str] = field(default_factory=set)
    has_error: bool = False
    start_ns: int | None = None
    end_ns: int | None = None

    @property
    def duration_ns(self) -> int:
        if self.start_ns is None or self.end_ns is None:
            return 0
        return self.end_ns - self.start_ns


@dataclass(frozen=True)
class TraceLabels:
    """Per-trace ground truth: injected anomaly labels, error status and slowness."""

    anomalies: frozenset[str]
    has_error: bool
    slow: bool

    @property
    def anomalous(self) -> bool:
        return bool(self.anomalies)


def _is_error(span: dict[str, Any]) -> bool:
    code = (span.get("status") or {}).get("code")
    return code in (2, "STATUS_CODE_ERROR", "ERROR")


def load_traces(path: Path) -> dict[str, TraceInfo]:
    """Read OTLP JSON lines (ExportTraceServiceRequest) into per-trace span sets."""
    traces: dict[str, TraceInfo] = defaultdict(TraceInfo)
    with path.open() as handle:
        for line in handle:
            if not line.strip():
                continue
            for resource in json.loads(line).get("resourceSpans", []):
                for scope in resource.get("scopeSpans", []):
                    for span in scope.get("spans", []):
                        info = traces[span["traceId"]]
                        info.spans.add(span["spanId"])
                        info.has_error |= _is_error(span)
                        start = int(span.get("startTimeUnixNano", 0))
                        end = int(span.get("endTimeUnixNano", start))
                        info.start_ns = (
                            start if info.start_ns is None else min(info.start_ns, start)
                        )
                        info.end_ns = end if info.end_ns is None else max(info.end_ns, end)
    return dict(traces)


def load_ground_truth(path: Path) -> dict[str, frozenset[str]]:
    """Map trace id to the set of anomaly labels injected anywhere in the trace."""
    labels: dict[str, set[str]] = defaultdict(set)
    with path.open() as handle:
        for line in handle:
            if line.strip():
                record = json.loads(line)
                labels[record["trace_id"]].update(record["anomalies"])
    return {trace_id: frozenset(found) for trace_id, found in labels.items()}


def _percentile(values: list[int], percentile: float) -> float:
    ordered = sorted(values)
    rank = max(0, math.ceil(percentile / 100 * len(ordered)) - 1)
    return ordered[rank]


def label_traces(
    reference: dict[str, TraceInfo],
    ground_truth: dict[str, frozenset[str]],
    slow_percentile: float = SLOW_PERCENTILE,
) -> dict[str, TraceLabels]:
    """Label every reference trace. Slow means at or above the reference duration percentile."""
    if not reference:
        return {}
    threshold = _percentile([t.duration_ns for t in reference.values()], slow_percentile)
    return {
        trace_id: TraceLabels(
            anomalies=ground_truth.get(trace_id, frozenset()),
            has_error=info.has_error,
            slow=info.duration_ns >= threshold and threshold > 0,
        )
        for trace_id, info in reference.items()
    }


def _recall(kept: set[str], wanted: set[str]) -> dict[str, float | int | None]:
    hit = len(kept & wanted)
    return {"total": len(wanted), "kept": hit, "recall": hit / len(wanted) if wanted else None}


def evaluate(
    reference: dict[str, TraceInfo],
    output: dict[str, TraceInfo],
    labels: dict[str, TraceLabels],
    reference_bytes: int = 0,
    output_bytes: int = 0,
) -> dict[str, Any]:
    """Reduction and retention metrics; output traces missing from the reference are ignored."""
    kept = {trace_id for trace_id in output if trace_id in reference}
    ref_spans = sum(len(t.spans) for t in reference.values())
    kept_spans = sum(len(output[t].spans & reference[t].spans) for t in kept)
    completeness = [
        len(output[t].spans & reference[t].spans) / len(reference[t].spans) for t in kept
    ]

    groups: dict[str, set[str]] = {
        "anomalous": {t for t, lab in labels.items() if lab.anomalous},
        "error": {t for t, lab in labels.items() if lab.has_error},
        "slow": {t for t, lab in labels.items() if lab.slow},
        "normal": {
            t for t, lab in labels.items() if not (lab.anomalous or lab.has_error or lab.slow)
        },
    }
    by_label: dict[str, set[str]] = defaultdict(set)
    for trace_id, lab in labels.items():
        for name in lab.anomalies:
            by_label[name].add(trace_id)

    anomalous_kept = len(kept & groups["anomalous"])
    return {
        "reference": {"traces": len(reference), "spans": ref_spans, "bytes": reference_bytes},
        "output": {"traces": len(kept), "spans": kept_spans, "bytes": output_bytes},
        "reduction": {
            "traces_kept": len(kept) / len(reference) if reference else None,
            "spans_kept": kept_spans / ref_spans if ref_spans else None,
            "bytes_kept": output_bytes / reference_bytes if reference_bytes else None,
            "mean_trace_completeness": sum(completeness) / len(completeness)
            if completeness
            else None,
        },
        "recall": {name: _recall(kept, wanted) for name, wanted in groups.items()},
        "recall_by_anomaly": {name: _recall(kept, by_label[name]) for name in sorted(by_label)},
        "precision": {"anomalous": anomalous_kept / len(kept) if kept else None},
    }


def _fmt(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.1%}"


def format_report(report: dict[str, Any]) -> str:
    ref, out, red = report["reference"], report["output"], report["reduction"]
    lines = [
        f"reference: {ref['traces']} traces, {ref['spans']} spans, {ref['bytes']} bytes",
        f"output:    {out['traces']} traces, {out['spans']} spans, {out['bytes']} bytes",
        f"traces kept {_fmt(red['traces_kept'])}, spans kept {_fmt(red['spans_kept'])}, "
        f"bytes kept {_fmt(red['bytes_kept'])}, mean trace completeness "
        f"{_fmt(red['mean_trace_completeness'])}",
        f"precision (kept traces that are anomalous): {_fmt(report['precision']['anomalous'])}",
        "",
        f"{'recall':<28}{'kept':>6}{'total':>7}{'recall':>9}",
    ]
    rows = list(report["recall"].items()) + [
        (f"anomaly:{name}", row) for name, row in report["recall_by_anomaly"].items()
    ]
    for name, row in rows:
        lines.append(f"{name:<28}{row['kept']:>6}{row['total']:>7}{_fmt(row['recall']):>9}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="mock-io-eval", description="Score collector output against a baseline capture."
    )
    parser.add_argument("--reference", type=Path, required=True, help="baseline_naive output")
    parser.add_argument("--output", type=Path, required=True, help="collector output to score")
    parser.add_argument(
        "--ground-truth", type=Path, required=True, help="MOCK_IO_GROUND_TRUTH_FILE"
    )
    parser.add_argument("--slow-percentile", type=float, default=SLOW_PERCENTILE)
    parser.add_argument("--json", action="store_true", help="print the report as JSON")
    args = parser.parse_args(argv)

    reference = load_traces(args.reference)
    ground_truth = load_ground_truth(args.ground_truth)
    labels = label_traces(reference, ground_truth, args.slow_percentile)
    report = evaluate(
        reference,
        load_traces(args.output),
        labels,
        args.reference.stat().st_size,
        args.output.stat().st_size,
    )
    unknown = len(set(ground_truth) - set(reference))
    if unknown:
        print(f"warning: {unknown} ground-truth traces are not in the reference", file=sys.stderr)
    print(json.dumps(report, indent=2) if args.json else format_report(report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
