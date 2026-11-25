import random

import pytest
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.common.trace_encoder import encode_spans
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import NonRecordingSpan, SpanContext, Status, StatusCode, TraceFlags

from mock_io_app.app_reducers import JobBufferProcessor, SpanFilterPathProcessor, Thresholds
from mock_io_app.replay import TraceTemplate, instantiate
from mock_io_app.replay_reducers import Reducer

HOUR = 3_600 * 10**9
SLOW_DB = 10**6  # db.query spans slower than 1 ms count as slow
TRACE_ID = 2**64 - 1  # low 64 bits at the maximum: never sampled below rate 1
THRESHOLDS = Thresholds(
    {
        ("job-svc", "db.query"): SLOW_DB,
        ("job-svc", "http.get"): HOUR,
        ("job-svc", "call_group.db.query"): HOUR,
        ("other-svc", "db.query"): SLOW_DB,
    }
)


class _Capture(SpanProcessor):
    """Records every finished span, in finishing order, like the baseline export."""

    def __init__(self) -> None:
        self.spans: list = []

    def on_end(self, span) -> None:
        self.spans.append(span)


def _remote(trace_id: int) -> trace.Context:
    parent = SpanContext(trace_id, 1, is_remote=True, trace_flags=TraceFlags(1))
    return trace.set_span_in_context(NonRecordingSpan(parent))


def _run_trace(processor: SpanProcessor, trace_id: int, scenario: str) -> list:
    """Spans of one trace: a run span with two jobs; `scenario` picks what is slow or failed."""
    capture = _Capture()
    resources = {
        name: Resource.create({"service.name": name})
        for name in ("run-svc", "job-svc", "other-svc")
    }
    providers = {}
    for name, resource in resources.items():
        provider = TracerProvider(resource=resource)
        provider.add_span_processor(processor)
        provider.add_span_processor(capture)
        providers[name] = provider.get_tracer("test")

    def db(tracer: trace.Tracer, *, slow: bool = False, fail: bool = False) -> None:
        with tracer.start_as_current_span("call_group.db.query"):
            for _ in range(2):
                start = 1_000
                span = tracer.start_span("db.query", start_time=start)
                if fail:
                    span.set_status(Status(StatusCode.ERROR))
                span.end(end_time=start + (2 * SLOW_DB if slow else 10))

    with providers["run-svc"].start_as_current_span("run.x", context=_remote(trace_id)) as run:
        for index in range(2):
            job = providers["job-svc"]
            # A job runs in its own process: its parent link to the run span is remote.
            run_context = run.get_span_context()
            remote_run = SpanContext(run_context.trace_id, run_context.span_id, True, TraceFlags(1))
            parent = trace.set_span_in_context(NonRecordingSpan(remote_run))
            with job.start_as_current_span(f"job.{index}", context=parent):
                with job.start_as_current_span("op.load"):
                    db(job, slow=scenario == "slow" and index == 0, fail=scenario == "error")
                    with job.start_as_current_span("http.get"):
                        pass
                    with job.start_as_current_span("unknown.detail"):
                        pass
                if index == 1:
                    other = providers["other-svc"]
                    with other.start_as_current_span("service.other.handle"):  # not in thresholds
                        db(other, slow=scenario == "slow_other")
    return capture.spans


def _template(spans: list) -> TraceTemplate:
    request = encode_spans(spans)
    template = TraceTemplate(trace_id=spans[0].context.trace_id.to_bytes(16, "big"), end_ns=0)
    for resource_spans in request.resource_spans:
        for scope_spans in resource_spans.scope_spans:
            single = type(resource_spans)()
            single.resource.CopyFrom(resource_spans.resource)
            single.scope_spans.add().CopyFrom(scope_spans)
            template.resource_spans.append(single)
    return template


def _run(name: str, scenario: str, rate: float) -> tuple[list, set[bytes]]:
    """One trace through the real processor: all its spans, and the IDs the processor forwarded."""
    exported = InMemorySpanExporter()
    downstream = SimpleSpanProcessor(exported)
    processor = (
        JobBufferProcessor(downstream, THRESHOLDS, rate)
        if name == "job_buffer"
        else SpanFilterPathProcessor(downstream, THRESHOLDS)
    )
    spans = _run_trace(processor, TRACE_ID, scenario)
    return spans, {s.context.span_id.to_bytes(8, "big") for s in exported.get_finished_spans()}


@pytest.mark.parametrize("name", ["job_buffer", "span_filter_path"])
@pytest.mark.parametrize("scenario", ["normal", "slow", "slow_other", "error"])
@pytest.mark.parametrize("rate", [0.0, 1.0])
def test_replay_reducer_keeps_the_same_spans_as_the_real_processor(
    name: str, scenario: str, rate: float
) -> None:
    spans, real = _run(name, scenario, rate)
    template = _template(spans)
    emulated = Reducer(name, THRESHOLDS, rate).kept_span_ids(template, TRACE_ID.to_bytes(16, "big"))
    assert emulated == real
    assert (
        len(real) < len(spans) or scenario != "normal" or rate == 1.0 or name == "span_filter_path"
    )


def test_instantiate_copies_only_kept_spans() -> None:
    template = _template(_run_trace(_Capture(), TRACE_ID, "slow"))
    reducer = Reducer("span_filter_path", THRESHOLDS)
    copies, _ = instantiate(template, 10**18, random.Random(1), reducer)
    full, _ = instantiate(template, 10**18, random.Random(1))
    sent = sum(len(c.scope_spans[0].spans) for c in copies)
    assert 0 < sent < sum(len(c.scope_spans[0].spans) for c in full)
    assert all(c.scope_spans[0].spans for c in copies)


def test_reducer_rejects_unknown_names() -> None:
    with pytest.raises(ValueError, match="unknown reducer"):
        Reducer("nope", THRESHOLDS)


def test_reduced_copy_carries_the_same_ids_as_the_full_copy() -> None:
    """A seeded reduced replay must be scorable against the seeded full replay."""
    template = _template(_run_trace(_Capture(), TRACE_ID, "slow"))
    reducer = Reducer("span_filter_path", THRESHOLDS)
    reduced, _ = instantiate(template, 10**18, random.Random(7), reducer)
    full, _ = instantiate(template, 10**18, random.Random(7))

    def ids(copies) -> set[bytes]:
        return {s.span_id for c in copies for s in c.scope_spans[0].spans}

    assert ids(reduced) < ids(full)
