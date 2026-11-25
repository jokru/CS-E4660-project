"""App-side trace reducers, run in shadow next to the full export.

Both wrap a downstream span processor (normally a BatchSpanProcessor with its own OTLP exporter)
and forward only part of the spans, so a run produces the full stream for the baseline plus one
reduced stream per reducer:

- `job_buffer`: buffers each local trace part (a job's spans in one process) until its local root
  ends, then forwards all of it if any span errored or was slow, or the trace is sampled, and
  drops it otherwise. Memory is bounded by the spans of the jobs currently running.
- `span_filter_path`: no buffering. Always forwards the skeleton (`run.`, `job.`, `op.` spans),
  and a detail span only if it errored, was slow, or has a forwarded descendant (children end
  first, so a kept span marks its parent to keep).

"Slow" compares a span's duration to a per (service.name, span name) threshold calibrated from a
baseline capture by scripts/calibrate_thresholds.py.
"""

import json
import os
import threading
from dataclasses import dataclass, field
from pathlib import Path

from opentelemetry.context import Context
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor
from opentelemetry.trace import StatusCode

SKELETON_PREFIXES = ("run.", "job.", "op.")
DEFAULT_SAMPLE_RATE = 0.05
DEFAULT_ENDPOINTS = {"job_buffer": "localhost:4319", "span_filter_path": "localhost:4320"}


@dataclass(frozen=True)
class Thresholds:
    """Latency thresholds in ns per (service.name, span name); unknown keys have none."""

    by_key: dict[tuple[str, str], int] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path) -> "Thresholds":
        data = json.loads(path.read_text())
        return cls(
            {(row["service"], row["name"]): int(row["threshold_ms"] * 1e6) for row in data["spans"]}
        )

    def get(self, span: ReadableSpan) -> int | None:
        service = span.resource.attributes.get("service.name", "") if span.resource else ""
        return self.by_key.get((str(service), span.name))


def duration_ns(span: ReadableSpan) -> int:
    return (span.end_time or 0) - (span.start_time or 0)


def is_error(span: ReadableSpan) -> bool:
    return span.status.status_code is StatusCode.ERROR


def is_slow(span: ReadableSpan, thresholds: Thresholds) -> bool:
    threshold = thresholds.get(span)
    return threshold is not None and duration_ns(span) >= threshold


def is_sampled(trace_id: int, rate: float) -> bool:
    """Deterministic per trace (trace ids are random), so every process agrees."""
    return (trace_id & (2**64 - 1)) < rate * 2**64


class _Forwarding(SpanProcessor):
    def __init__(self, downstream: SpanProcessor) -> None:
        self.downstream = downstream

    def shutdown(self) -> None:
        self.downstream.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return self.downstream.force_flush(timeout_millis)


@dataclass
class _Part:
    spans: list[ReadableSpan] = field(default_factory=list)
    started: set[int] = field(default_factory=set)
    interesting: bool = False


class JobBufferProcessor(_Forwarding):
    def __init__(
        self,
        downstream: SpanProcessor,
        thresholds: Thresholds,
        sample_rate: float = DEFAULT_SAMPLE_RATE,
    ) -> None:
        super().__init__(downstream)
        self.thresholds = thresholds
        self.sample_rate = sample_rate
        self._lock = threading.Lock()
        self._root_of: dict[int, int] = {}
        self._parts: dict[int, _Part] = {}

    def on_start(self, span: Span, parent_context: Context | None = None) -> None:
        span_id = span.context.span_id
        parent = span.parent
        with self._lock:
            root = span_id
            if parent is not None and not parent.is_remote:
                root = self._root_of.get(parent.span_id, span_id)
            self._root_of[span_id] = root
            self._parts.setdefault(root, _Part()).started.add(span_id)

    def on_end(self, span: ReadableSpan) -> None:
        span_id = span.context.span_id
        with self._lock:
            root = self._root_of.get(span_id)
            part = self._parts.get(root) if root is not None else None
            if part is None:
                return
            part.spans.append(span)
            part.interesting |= is_error(span) or is_slow(span, self.thresholds)
            if span_id != root:
                return
            del self._parts[root]
            for started in part.started:
                self._root_of.pop(started, None)
        if part.interesting or is_sampled(span.context.trace_id, self.sample_rate):
            for buffered in part.spans:
                self.downstream.on_end(buffered)

    @property
    def buffered_spans(self) -> int:
        with self._lock:
            return sum(len(p.spans) for p in self._parts.values())


class SpanFilterPathProcessor(_Forwarding):
    def __init__(self, downstream: SpanProcessor, thresholds: Thresholds) -> None:
        super().__init__(downstream)
        self.thresholds = thresholds
        self._lock = threading.Lock()
        self._keep: set[int] = set()

    def on_end(self, span: ReadableSpan) -> None:
        with self._lock:
            marked = span.context.span_id in self._keep
            self._keep.discard(span.context.span_id)
        # Detail spans without a calibrated threshold are kept: there is nothing to compare to.
        keep = (
            marked
            or span.name.startswith(SKELETON_PREFIXES)
            or is_error(span)
            or self.thresholds.get(span) is None
            or is_slow(span, self.thresholds)
        )
        if not keep:
            return
        parent = span.parent
        if (
            parent is not None
            and not parent.is_remote
            and not span.name.startswith(SKELETON_PREFIXES)
        ):
            with self._lock:
                self._keep.add(parent.span_id)
        self.downstream.on_end(span)


def build_app_reducers(spec: str) -> list[SpanProcessor]:
    """Reducers named in MOCK_IO_APP_REDUCERS (comma separated), each with its own OTLP export.

    Thresholds come from MOCK_IO_THRESHOLDS; endpoints from MOCK_IO_<NAME>_ENDPOINT.
    """
    names = [n.strip() for n in spec.split(",") if n.strip()]
    if not names:
        return []
    unknown = set(names) - set(DEFAULT_ENDPOINTS)
    if unknown:
        raise ValueError(f"unknown MOCK_IO_APP_REDUCERS {sorted(unknown)}")
    thresholds_path = os.environ.get("MOCK_IO_THRESHOLDS")
    if not thresholds_path:
        raise ValueError("MOCK_IO_APP_REDUCERS needs MOCK_IO_THRESHOLDS (a thresholds JSON)")
    thresholds = Thresholds.load(Path(thresholds_path))

    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    reducers: list[SpanProcessor] = []
    for name in names:
        endpoint = os.environ.get(f"MOCK_IO_{name.upper()}_ENDPOINT", DEFAULT_ENDPOINTS[name])
        downstream = BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True))
        if name == "job_buffer":
            rate = float(os.environ.get("MOCK_IO_JOB_BUFFER_SAMPLE_RATE", DEFAULT_SAMPLE_RATE))
            reducers.append(JobBufferProcessor(downstream, thresholds, rate))
        else:
            reducers.append(SpanFilterPathProcessor(downstream, thresholds))
    return reducers
