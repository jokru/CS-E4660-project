"""Replay a recorded capture over OTLP at a high rate, without running the app.

Each sent copy of a recorded trace gets new trace and span IDs and is shifted so it ends now;
structure, latencies, status and attributes are kept. A matching ground-truth file is written
under the new IDs, so `mock-io-eval` scores replayed workloads like recorded ones.
"""

import argparse
import base64
import json
import os
import random
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, TextIO
from urllib.parse import urlparse

from google.protobuf.json_format import ParseDict
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.trace.v1.trace_pb2 import ResourceSpans

from mock_io_app.app_reducers import DEFAULT_SAMPLE_RATE, Thresholds
from mock_io_app.replay_reducers import REDUCERS, Reducer
from mock_io_app.tracing import RUN_JOB_ATTRIBUTE

_ID_FIELDS = ("traceId", "spanId", "parentSpanId")


@dataclass
class TraceTemplate:
    trace_id: bytes
    end_ns: int
    resource_spans: list[ResourceSpans] = field(default_factory=list)
    span_count: int = 0
    # Ground-truth records of this trace, as written by MOCK_IO_GROUND_TRUTH_FILE.
    ground_truth: list[dict[str, Any]] = field(default_factory=list)


def _hex_ids_to_base64(span: dict[str, Any]) -> dict[str, Any]:
    """OTLP JSON uses hex IDs; protobuf's JSON parser expects base64 for bytes fields."""
    span = dict(span)
    for key in _ID_FIELDS:
        if span.get(key):
            span[key] = base64.b64encode(bytes.fromhex(span[key])).decode()
    if span.get("links"):
        span["links"] = [_hex_ids_to_base64(link) for link in span["links"]]
    return span


def load_capture(path: Path, ground_truth: Path | None = None) -> list[TraceTemplate]:
    """Read OTLP JSON lines into one template per trace, ordered by trace start."""
    grouped: dict[str, dict[tuple[str, str], list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    scopes: dict[tuple[str, str], tuple[dict[str, Any], dict[str, Any]]] = {}
    starts: dict[str, int] = {}
    with path.open() as handle:
        for line in handle:
            if not line.strip():
                continue
            for resource_spans in json.loads(line).get("resourceSpans", []):
                resource = resource_spans.get("resource", {})
                for scope_spans in resource_spans.get("scopeSpans", []):
                    scope = scope_spans.get("scope", {})
                    key = (json.dumps(resource, sort_keys=True), json.dumps(scope, sort_keys=True))
                    scopes.setdefault(key, (resource, scope))
                    for span in scope_spans.get("spans", []):
                        trace_id = span["traceId"]
                        grouped[trace_id][key].append(_hex_ids_to_base64(span))
                        start = int(span.get("startTimeUnixNano", 0))
                        starts[trace_id] = min(starts.get(trace_id, start), start)

    records: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if ground_truth is not None:
        with ground_truth.open() as handle:
            for line in handle:
                if line.strip():
                    record = json.loads(line)
                    records[record["trace_id"]].append(record)

    templates = []
    for trace_id in sorted(grouped, key=starts.__getitem__):
        template = TraceTemplate(
            trace_id=bytes.fromhex(trace_id), end_ns=0, ground_truth=records.get(trace_id, [])
        )
        _add_run_job(grouped[trace_id])
        for key, spans in grouped[trace_id].items():
            resource, scope = scopes[key]
            message = ParseDict(
                {"resource": resource, "scopeSpans": [{"scope": scope, "spans": spans}]},
                ResourceSpans(),
            )
            template.resource_spans.append(message)
            for span in message.scope_spans[0].spans:
                template.end_ns = max(template.end_ns, span.end_time_unix_nano)
                template.span_count += 1
        templates.append(template)
    return templates


def _add_run_job(spans_by_scope: dict[tuple[str, str], list[dict[str, Any]]]) -> None:
    """Captures from before the app set RUN_JOB_ATTRIBUTE get it from the trace's `run.*` span."""
    spans = [span for group in spans_by_scope.values() for span in group]
    jobs = {s["name"].removeprefix("run.") for s in spans if s["name"].startswith("run.")}
    if len(jobs) != 1:
        return
    attribute = {"key": RUN_JOB_ATTRIBUTE, "value": {"stringValue": jobs.pop()}}
    for span in spans:
        attributes = span.setdefault("attributes", [])
        if not any(a["key"] == RUN_JOB_ATTRIBUTE for a in attributes):
            attributes.append(attribute)


def instantiate(
    template: TraceTemplate, now_ns: int, rng: random.Random, reducer: "Reducer | None" = None
) -> tuple[list[ResourceSpans], list[dict[str, Any]]]:
    """Copy a trace with fresh IDs, shifted to end at now_ns; return spans and ground truth.

    With a `reducer`, only the spans it keeps are copied (ground truth is kept whole).
    """
    trace_id = rng.randbytes(16)
    kept = reducer.kept_span_ids(template, trace_id) if reducer else None
    shift = now_ns - template.end_ns
    span_ids: dict[bytes, bytes] = {}

    def new_span_id(old: bytes) -> bytes:
        if old not in span_ids:
            span_ids[old] = rng.randbytes(8)
        return span_ids[old]

    copies = []
    for original in template.resource_spans:
        copy = ResourceSpans()
        copy.CopyFrom(original)
        dropped: list[int] = []
        for index, span in enumerate(copy.scope_spans[0].spans):
            if kept is not None and span.span_id not in kept:
                dropped.append(index)
            # IDs are assigned for every span, kept or not, so a reduced copy carries the same
            # IDs as the full copy of the same seeded replay (needed to score it).
            span.trace_id = trace_id
            span.span_id = new_span_id(span.span_id)
            if span.parent_span_id:
                span.parent_span_id = new_span_id(span.parent_span_id)
            span.start_time_unix_nano += shift
            span.end_time_unix_nano += shift
            for event in span.events:
                event.time_unix_nano += shift
            for link in span.links:
                if link.trace_id == template.trace_id:
                    link.trace_id = trace_id
                    link.span_id = new_span_id(link.span_id)
        for index in reversed(dropped):
            del copy.scope_spans[0].spans[index]
        if dropped and not copy.scope_spans[0].spans:
            continue
        copies.append(copy)

    records = []
    for record in template.ground_truth:
        record = dict(record)
        record["trace_id"] = trace_id.hex()
        old_span = bytes.fromhex(record["span_id"])
        record["span_id"] = span_ids.get(old_span, old_span).hex()
        if "timestamp" in record:
            record["timestamp"] += shift / 1e9
        records.append(record)
    return copies, records


class Exporter(Protocol):
    def __call__(self, request: ExportTraceServiceRequest) -> None: ...


def grpc_exporter(endpoint: str, timeout: float = 10.0) -> Exporter:
    import grpc
    from opentelemetry.proto.collector.trace.v1.trace_service_pb2_grpc import TraceServiceStub

    parsed = urlparse(endpoint if "://" in endpoint else f"http://{endpoint}")
    target = f"{parsed.hostname}:{parsed.port or 4317}"
    channel = (
        grpc.secure_channel(target, grpc.ssl_channel_credentials())
        if parsed.scheme == "https"
        else grpc.insecure_channel(target)
    )
    stub = TraceServiceStub(channel)

    def export(request: ExportTraceServiceRequest) -> None:
        stub.Export(request, timeout=timeout)

    return export


@dataclass
class ReplayStats:
    traces: int = 0
    spans: int = 0
    requests: int = 0
    failed_requests: int = 0
    failed_spans: int = 0
    errors: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    elapsed: float = 0.0


def replay(
    templates: list[TraceTemplate],
    export: Exporter,
    *,
    traces: int,
    rate: float | None,
    workers: int = 4,
    batch: int = 1,
    seed: int | None = None,
    shuffle: bool = False,
    ground_truth_out: TextIO | None = None,
    clock: Any = time,
    reducer: "Reducer | None" = None,
) -> ReplayStats:
    """Send `traces` copies cycling through `templates`, at `rate` traces/s (None = flat out).

    Workers send requests of `batch` traces each, scheduled at the target rate; a worker that
    falls behind sends immediately, so the achieved rate shows when the sender is the limit.
    """
    if not templates:
        raise ValueError("capture has no traces")
    order = list(range(len(templates)))
    if shuffle:
        random.Random(seed).shuffle(order)
    stats = ReplayStats()
    lock = threading.Lock()
    next_index = 0
    started = clock.monotonic()

    def worker() -> None:
        nonlocal next_index
        while True:
            with lock:
                first = next_index
                if first >= traces:
                    return
                count = min(batch, traces - first)
                next_index += count
            if rate:
                delay = started + first / rate - clock.monotonic()
                if delay > 0:
                    clock.sleep(delay)
            request = ExportTraceServiceRequest()
            records: list[dict[str, Any]] = []
            spans = 0
            now_ns = time.time_ns()
            for index in range(first, first + count):
                template = templates[order[index % len(order)]]
                # With a seed, IDs depend only on the seed and position, so separate replays
                # (e.g. one collector at a time) send the same traces and ground truth.
                rng = random.Random(f"{seed}:{index}" if seed is not None else os.urandom(16))
                copies, trace_records = instantiate(template, now_ns, rng, reducer)
                request.resource_spans.extend(copies)
                records += trace_records
                spans += sum(len(scope.spans) for copy in copies for scope in copy.scope_spans)
            try:
                export(request)
                failed = None
            except Exception as error:  # noqa: BLE001 - counted and reported, not retried
                failed = getattr(error, "code", lambda: None)()
                failed = str(failed) if failed is not None else type(error).__name__
            with lock:
                stats.requests += 1
                stats.traces += count
                stats.spans += spans
                if failed:
                    stats.failed_requests += 1
                    stats.failed_spans += spans
                    stats.errors[failed] += 1
                elif ground_truth_out is not None:
                    for record in records:
                        ground_truth_out.write(json.dumps(record) + "\n")

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for future in [pool.submit(worker) for _ in range(workers)]:
            future.result()
    stats.elapsed = clock.monotonic() - started
    return stats


def format_stats(stats: ReplayStats) -> str:
    seconds = max(stats.elapsed, 1e-9)
    lines = [
        f"sent {stats.traces} traces, {stats.spans} spans in {stats.requests} requests "
        f"over {stats.elapsed:.1f}s",
        f"achieved {stats.traces / seconds:.1f} traces/s, {stats.spans / seconds:.0f} spans/s",
    ]
    if stats.failed_requests:
        errors = ", ".join(f"{name}: {n}" for name, n in sorted(stats.errors.items()))
        lines.append(
            f"failed {stats.failed_requests} requests, {stats.failed_spans} spans ({errors})"
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="mock-io-replay",
        description="Replay a recorded capture over OTLP gRPC at a target rate.",
    )
    parser.add_argument("--capture", type=Path, required=True, help="recorded baseline_naive.jsonl")
    parser.add_argument("--ground-truth", type=Path, help="ground truth of the recorded capture")
    parser.add_argument(
        "--ground-truth-out", type=Path, help="write ground truth of the replayed traces here"
    )
    parser.add_argument("--traces", type=int, help="traces to send (default: one pass)")
    parser.add_argument(
        "--rate", type=float, help="traces per second (default: as fast as possible)"
    )
    parser.add_argument("--workers", type=int, default=4, help="concurrent senders")
    parser.add_argument("--batch", type=int, default=1, help="traces per export request")
    parser.add_argument("--shuffle", action="store_true", help="random trace order (see --seed)")
    parser.add_argument(
        "--seed",
        type=int,
        help="seed for the order (--shuffle) and IDs; same seed = same traces",
    )
    parser.add_argument(
        "--endpoint",
        default=os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4317"),
        help="OTLP gRPC endpoint (default: OTEL_EXPORTER_OTLP_ENDPOINT or 127.0.0.1:4317)",
    )
    parser.add_argument(
        "--reducer",
        choices=REDUCERS,
        help="send only what this app-side reducer would forward (needs --thresholds)",
    )
    parser.add_argument("--thresholds", type=Path, help="thresholds JSON for --reducer")
    parser.add_argument(
        "--sample-rate", type=float, default=DEFAULT_SAMPLE_RATE, help="job_buffer random keep rate"
    )
    args = parser.parse_args(argv)
    if args.reducer and not args.thresholds:
        parser.error("--reducer needs --thresholds")
    if args.ground_truth_out and not args.ground_truth:
        parser.error("--ground-truth-out needs --ground-truth")
    for name in ("workers", "batch", "traces"):
        if getattr(args, name) is not None and getattr(args, name) < 1:
            parser.error(f"--{name.replace('_', '-')} must be at least 1")
    if args.rate is not None and args.rate <= 0:
        parser.error("--rate must be positive")
    capture, out = args.capture.resolve(), args.ground_truth_out
    if out and out.resolve() in {capture, args.ground_truth.resolve()}:
        parser.error("--ground-truth-out must differ from the inputs")

    reducer = (
        Reducer(args.reducer, Thresholds.load(args.thresholds), args.sample_rate)
        if args.reducer
        else None
    )
    templates = load_capture(args.capture, args.ground_truth)
    if not templates:
        parser.error(f"no traces in {args.capture}")
    spans = sum(t.span_count for t in templates)
    print(f"loaded {len(templates)} traces, {spans} spans from {args.capture}")
    handle = out.open("w") if out else None
    try:
        stats = replay(
            templates,
            grpc_exporter(args.endpoint),
            traces=args.traces or len(templates),
            rate=args.rate,
            workers=args.workers,
            batch=args.batch,
            seed=args.seed,
            shuffle=args.shuffle,
            ground_truth_out=handle,
            reducer=reducer,
        )
    finally:
        if handle:
            handle.close()
    print(format_stats(stats))
    return 1 if stats.failed_requests else 0


if __name__ == "__main__":
    raise SystemExit(main())
