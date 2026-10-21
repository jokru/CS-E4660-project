from collections import Counter

import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic import ValidationError

from mock_io_app.job_factory import build_job, default_resources
from mock_io_app.scenario import Scenario, closure, expand_jobs, leaf_jobs


def _job(name: str, depends_on: list[str] | None = None, copies: int = 1, count: int = 1) -> dict:
    return {
        "name": name,
        "copies": copies,
        "depends_on": depends_on or [],
        "ops": [{"name": "op", "call_groups": [{"io": "db", "action": "query", "count": count}]}],
    }


def _scenario(*jobs: dict, failure_rate: float = 0.0) -> Scenario:
    defaults = {"latency": {"dist": "fixed", "ms": 0}, "failure_rate": failure_rate}
    return Scenario.model_validate({"defaults": defaults, "jobs": list(jobs)})


def _build(scenario: Scenario):
    expanded = expand_jobs("demo", scenario)
    by_name = {j.name: j for j in expanded}
    return {j.name: build_job(j, by_name) for j in expanded}


def test_upstream_expands_to_all_copies() -> None:
    jobs = expand_jobs("demo", _scenario(_job("a", copies=2), _job("b", ["a"])))
    b = next(j for j in jobs if j.name == "demo__b")
    assert b.upstream == ("demo__a_0", "demo__a_1")


@pytest.mark.parametrize(
    ("jobs", "message"),
    [
        ([_job("a", ["a"])], "depends on itself"),
        ([_job("a", ["missing"])], "unknown job"),
        ([_job("a", ["b"]), _job("b", ["a"])], "cycle"),
        ([_job("a"), _job("b", ["a", "a"])], "twice"),
    ],
)
def test_invalid_dependencies_rejected(jobs: list[dict], message: str) -> None:
    with pytest.raises(ValidationError, match=message):
        _scenario(*jobs)


def test_op_count_is_capped() -> None:
    jobs = [_job("a0")] + [_job(f"a{i}", [f"a{i - 1}"], copies=10) for i in range(1, 5)]
    with pytest.raises(ValidationError, match="more than"):
        _scenario(*jobs)


def test_closure_orders_upstream_first_and_leaves_exclude_dependencies() -> None:
    expanded = expand_jobs("demo", _scenario(_job("c", ["b"]), _job("b", ["a"]), _job("a")))
    by_name = {j.name: j for j in expanded}
    names = [j.name for j in closure(by_name["demo__c"], by_name)]
    assert names == ["demo__a", "demo__b", "demo__c"]
    assert [j.name for j in leaf_jobs(expanded)] == ["demo__c"]


def test_dependent_job_runs_upstream_ops_first(spans: InMemorySpanExporter) -> None:
    jobs = _build(_scenario(_job("a", count=2, copies=2), _job("b", ["a"], count=3)))
    assert jobs["demo__b"].execute_in_process(resources=default_resources()).success

    finished = spans.get_finished_spans()
    counts = Counter(s.name for s in finished)
    assert counts["op.op"] == 3  # a_0, a_1 and b in a single run
    assert counts["db.query"] == 2 * 2 + 3

    ops = sorted((s for s in finished if s.name == "op.op"), key=lambda s: s.start_time)
    assert ops[-1].attributes["mock_io.source_job"] == "demo__b"
    assert {s.attributes["dagster.job"] for s in ops} == {"demo__b"}
    assert max(s.end_time for s in ops[:2]) <= ops[2].start_time


def test_standalone_job_does_not_include_dependents(spans: InMemorySpanExporter) -> None:
    jobs = _build(_scenario(_job("a"), _job("b", ["a"])))
    assert jobs["demo__a"].execute_in_process(resources=default_resources()).success
    assert Counter(s.name for s in spans.get_finished_spans())["op.op"] == 1


def test_upstream_failure_skips_downstream_ops() -> None:
    jobs = _build(_scenario(_job("a"), _job("b", ["a"]), failure_rate=1.0))
    result = jobs["demo__b"].execute_in_process(resources=default_resources(), raise_on_error=False)
    assert not result.success
    succeeded = [e.step_key for e in result.all_events if e.is_step_success]
    assert not succeeded
