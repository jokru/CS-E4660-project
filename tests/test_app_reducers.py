from collections.abc import Iterator

import pytest
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import NonRecordingSpan, SpanContext, Status, StatusCode, TraceFlags

from mock_io_app.app_reducers import (
    JobBufferProcessor,
    SpanFilterPathProcessor,
    Thresholds,
    is_sampled,
)
from mock_io_app.tracing import RunJobAttributeProcessor, for_run_job

# Thresholds of 1 hour (never slow) or 0 ns (always slow) keep the tests independent of timing.
FAST = {("svc", "db.query"): 3_600 * 10**9, ("svc", "call_group.db.query"): 3_600 * 10**9}
SLOW = {("svc", "db.query"): 0, ("svc", "call_group.db.query"): 3_600 * 10**9}
UNSAMPLED_TRACE = 2**64 - 1  # low 64 bits at the maximum: never sampled below rate 1


def _tracer(processor: SpanProcessor) -> trace.Tracer:
    provider = TracerProvider(resource=Resource.create({"service.name": "svc"}))
    provider.add_span_processor(processor)
    return provider.get_tracer("test")


@pytest.fixture
def exported() -> Iterator[InMemorySpanExporter]:
    yield InMemorySpanExporter()


def _remote_parent(trace_id: int = UNSAMPLED_TRACE) -> trace.Context:
    parent = SpanContext(trace_id, 1, is_remote=True, trace_flags=TraceFlags(1))
    return trace.set_span_in_context(NonRecordingSpan(parent))


def _job(tracer: trace.Tracer, *, fail: bool = False) -> None:
    with (
        tracer.start_as_current_span("job.a", context=_remote_parent()),
        tracer.start_as_current_span("call_group.db.query"),
        tracer.start_as_current_span("db.query") as leaf,
    ):
        if fail:
            leaf.set_status(Status(StatusCode.ERROR))


@pytest.mark.parametrize(
    ("thresholds", "fail", "kept"),
    [(FAST, False, 0), (FAST, True, 3), (SLOW, False, 3)],
    ids=["normal-dropped", "error-keeps-job", "slow-keeps-job"],
)
def test_job_buffer_forwards_a_whole_job_part_or_nothing(
    exported: InMemorySpanExporter, thresholds: dict, fail: bool, kept: int
) -> None:
    buffer = JobBufferProcessor(SimpleSpanProcessor(exported), Thresholds(thresholds), 0.0)
    _job(_tracer(buffer), fail=fail)

    assert len(exported.get_finished_spans()) == kept
    assert buffer.buffered_spans == 0


def test_job_buffer_keeps_sampled_traces(exported: InMemorySpanExporter) -> None:
    buffer = JobBufferProcessor(SimpleSpanProcessor(exported), Thresholds(FAST), 1.0)
    _job(_tracer(buffer))
    assert len(exported.get_finished_spans()) == 3
    assert is_sampled(0, 0.05) and not is_sampled(UNSAMPLED_TRACE, 0.05)


def test_job_buffer_decides_each_local_root_separately(exported: InMemorySpanExporter) -> None:
    tracer = _tracer(JobBufferProcessor(SimpleSpanProcessor(exported), Thresholds(FAST), 0.0))
    with tracer.start_as_current_span("job.ok", context=_remote_parent()):
        tracer.start_span("db.query").end()
    _job(tracer, fail=True)
    assert {s.name for s in exported.get_finished_spans()} == {
        "job.a",
        "call_group.db.query",
        "db.query",
    }


def test_span_filter_path_keeps_skeleton_and_the_path_to_interesting_spans(
    exported: InMemorySpanExporter,
) -> None:
    tracer = _tracer(SpanFilterPathProcessor(SimpleSpanProcessor(exported), Thresholds(FAST)))
    with (
        tracer.start_as_current_span("job.a", context=_remote_parent()),
        tracer.start_as_current_span("op.x"),
    ):
        with tracer.start_as_current_span("call_group.db.query"):
            tracer.start_span("db.query").end()
        with tracer.start_as_current_span("call_group.db.query"):
            with tracer.start_as_current_span("db.query") as failed:
                failed.set_status(Status(StatusCode.ERROR))
            tracer.start_span("db.query").end()
        tracer.start_span("http.get").end()  # no threshold: kept

    names = [s.name for s in exported.get_finished_spans()]
    assert names == ["db.query", "call_group.db.query", "http.get", "op.x", "job.a"]
    error_parent = exported.get_finished_spans()[1]
    assert exported.get_finished_spans()[0].parent.span_id == error_parent.context.span_id


def test_run_job_attribute_is_set_inside_for_run_job(exported: InMemorySpanExporter) -> None:
    provider = TracerProvider()
    provider.add_span_processor(RunJobAttributeProcessor())
    provider.add_span_processor(SimpleSpanProcessor(exported))
    tracer = provider.get_tracer("test")
    with for_run_job("daily"):
        tracer.start_span("inside").end()
    tracer.start_span("outside").end()

    inside, outside = exported.get_finished_spans()
    assert inside.attributes["mock_io.run_job"] == "daily"
    assert "mock_io.run_job" not in outside.attributes
