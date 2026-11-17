from collections import Counter
from pathlib import Path

import yaml
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from mock_io_app.definitions import build_definitions
from mock_io_app.job_factory import build_job, default_resources
from mock_io_app.scenario import Scenario, expand_jobs
from mock_io_app.tracing import get_tracer, trace_tags
from tests.conftest import zero_latency_scenario


def _jobs(raw: dict) -> list:
    expanded = expand_jobs("demo", Scenario.model_validate(raw))
    by_name = {e.name: e for e in expanded}
    return [build_job(e, by_name) for e in expanded]


def test_jobs_run_and_emit_expected_spans(spans: InMemorySpanExporter) -> None:
    for job_def in _jobs(zero_latency_scenario()):
        assert job_def.execute_in_process(resources=default_resources()).success

    counts = Counter(span.name for span in spans.get_finished_spans())
    # alpha has 2 copies x (4 http.get + 2 db.query + 3 gcs.upload); beta has 1 db.insert
    assert counts["http.get"] == 8
    assert counts["db.query"] == 4
    assert counts["gcs.upload"] == 6
    assert counts["db.insert"] == 1
    assert counts["op.fetch"] == 2
    assert counts["op.store"] == 2
    assert counts["op.only"] == 1


def test_ops_continue_trace_from_run_tags(spans: InMemorySpanExporter) -> None:
    job_def = _jobs(zero_latency_scenario())[-1]
    with get_tracer().start_as_current_span("parent") as parent:
        tags = trace_tags()
    assert job_def.execute_in_process(resources=default_resources(), tags=tags).success

    finished = spans.get_finished_spans()
    job_span = next(s for s in finished if s.name == "job.demo__beta")
    op_span = next(s for s in finished if s.name == "op.only")
    assert job_span.context.trace_id == parent.get_span_context().trace_id
    assert job_span.parent is not None
    assert job_span.parent.span_id == parent.get_span_context().span_id
    assert op_span.parent is not None
    assert op_span.parent.span_id == job_span.context.span_id


def test_failure_rate_one_fails_run() -> None:
    raw = zero_latency_scenario()
    raw["defaults"]["failure_rate"] = 1.0
    job_def = _jobs(raw)[-1]
    result = job_def.execute_in_process(resources=default_resources(), raise_on_error=False)
    assert not result.success


def test_build_definitions_from_directory(tmp_path: Path) -> None:
    (tmp_path / "demo.yaml").write_text(yaml.safe_dump(zero_latency_scenario()))
    defs = build_definitions(tmp_path, "*.yaml")
    names = sorted(job.name for job in defs.resolve_all_job_defs())
    assert names == ["demo__alpha_0", "demo__alpha_1", "demo__beta"]


def test_op_retries_rerun_inside_the_job_span(spans: InMemorySpanExporter) -> None:
    raw = zero_latency_scenario()
    raw["defaults"]["failure_rate"] = 1.0
    raw["jobs"][1]["ops"][0]["retries"] = 2
    job_def = _jobs(raw)[-1]
    assert not job_def.execute_in_process(
        resources=default_resources(), raise_on_error=False
    ).success

    finished = spans.get_finished_spans()
    attempts = sorted(s.attributes["dagster.retry_number"] for s in finished if s.name == "op.only")
    assert attempts == [0, 1, 2]
    assert sum(s.name == "job.demo__beta" for s in finished) == 1
