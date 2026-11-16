import asyncio
import json
from collections.abc import Mapping
from pathlib import Path

import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.util.types import AttributeValue
from pydantic import PrivateAttr

from mock_io_app.executor import IOResources, build_call_params, make_rng, run_call_group
from mock_io_app.resources import (
    CallParams,
    FakeDatabase,
    FakeGCS,
    FakeHttpClient,
    MockIOError,
)
from mock_io_app.scenario import CallGroupSpec, Defaults, Scenario

ZERO_DEFAULTS = Defaults.model_validate({"latency": {"dist": "fixed", "ms": 0}})


class TrackingDatabase(FakeDatabase):
    _in_flight: int = PrivateAttr(default=0)
    _peak: int = PrivateAttr(default=0)
    _calls: int = PrivateAttr(default=0)

    async def _simulate(
        self,
        io_type: str,
        action: str,
        params: CallParams,
        attributes: Mapping[str, AttributeValue] | None = None,
    ) -> None:
        self._calls += 1
        self._in_flight += 1
        self._peak = max(self._peak, self._in_flight)
        try:
            await asyncio.sleep(0.002)
        finally:
            self._in_flight -= 1
        if params.fail:
            raise MockIOError("boom")


def _resources(db: FakeDatabase | None = None) -> IOResources:
    return IOResources(db=db or FakeDatabase(), gcs=FakeGCS(), http=FakeHttpClient())


def _group(**kwargs: object) -> CallGroupSpec:
    return CallGroupSpec.model_validate({"io": "db", "action": "query", **kwargs})


async def test_gather_respects_max_concurrency() -> None:
    db = TrackingDatabase()
    group = _group(count=12, mode="gather", max_concurrency=3)
    await run_call_group(group, ZERO_DEFAULTS, _resources(db), make_rng(1), "k")
    assert db._calls == 12
    assert db._peak == 3


async def test_sequential_runs_one_at_a_time() -> None:
    db = TrackingDatabase()
    await run_call_group(_group(count=5), ZERO_DEFAULTS, _resources(db), make_rng(1), "k")
    assert db._calls == 5
    assert db._peak == 1


@pytest.mark.parametrize("mode", ["sequential", "gather"])
async def test_failure_rate_one_raises(mode: str) -> None:
    group = _group(count=3, mode=mode, failure_rate=1.0)
    with pytest.raises(MockIOError):
        await run_call_group(group, ZERO_DEFAULTS, _resources(TrackingDatabase()), make_rng(1), "k")


def test_call_params_are_reproducible_per_seed() -> None:
    group = _group(count=20, latency={"dist": "lognormal", "median_ms": 10}, failure_rate=0.5)
    a = build_call_params(group, ZERO_DEFAULTS, make_rng(7, "job", "op", 0))
    b = build_call_params(group, ZERO_DEFAULTS, make_rng(7, "job", "op", 0))
    c = build_call_params(group, ZERO_DEFAULTS, make_rng(7, "job", "op", 1))
    assert a == b
    assert a != c


@pytest.mark.parametrize(
    ("io", "action"),
    [
        ("db", "query"),
        ("db", "insert"),
        ("gcs", "upload"),
        ("gcs", "download"),
        ("gcs", "list"),
        ("http", "get"),
        ("http", "post"),
    ],
)
async def test_every_action_emits_one_span_per_call(
    io: str, action: str, spans: InMemorySpanExporter
) -> None:
    group = CallGroupSpec.model_validate({"io": io, "action": action, "count": 3, "mode": "gather"})
    await run_call_group(group, ZERO_DEFAULTS, _resources(), make_rng(1), "k")
    names = [span.name for span in spans.get_finished_spans()]
    assert names.count(f"{io}.{action}") == 3
    assert names.count(f"call_group.{io}.{action}") == 1


def test_anomaly_applies_effects_and_labels_calls() -> None:
    anomaly = {"label": "spike", "latency_multiplier": 10, "latency_add_ms": 5, "fail": True}
    group = _group(count=3, anomalies=[{**anomaly, "payload_bytes": 99}])
    params = build_call_params(group, ZERO_DEFAULTS, make_rng(1))
    assert [(p.latency_s, p.fail, p.payload_bytes, p.anomalies) for p in params] == [
        (0.005, True, 99, ("spike",))
    ] * 3


def test_anomaly_respects_iteration_window_and_probability() -> None:
    window = {"label": "w", "fail": True, "from_iteration": 2, "to_iteration": 4}
    group = _group(count=5, anomalies=[window])
    for iteration, expected in [(1, False), (2, True), (3, True), (4, False)]:
        params = build_call_params(group, ZERO_DEFAULTS, make_rng(1), iteration)
        assert all(p.fail is expected for p in params)

    flaky = _group(count=200, anomalies=[{"label": "f", "probability": 0.3, "fail": True}])
    hit = sum(bool(p.anomalies) for p in build_call_params(flaky, ZERO_DEFAULTS, make_rng(1)))
    assert 30 < hit < 90


def test_service_anomalies_label_service_spans(spans: InMemorySpanExporter) -> None:
    scenario = Scenario.model_validate(
        {
            "defaults": {"latency": {"dist": "fixed", "ms": 0}},
            "services": {
                "tax": {"kind": "third_party", "anomalies": [{"label": "slow_tax", "fail": True}]}
            },
            "jobs": [
                {
                    "name": "j",
                    "ops": [
                        {
                            "name": "o",
                            "call_groups": [{"io": "service", "action": "call", "service": "tax"}],
                        }
                    ],
                }
            ],
        }
    )
    group = scenario.jobs[0].ops[0].call_groups[0]
    with pytest.raises(MockIOError):
        asyncio.run(
            run_call_group(
                group, scenario.defaults, _resources(), make_rng(1), "k", scenario.services
            )
        )
    call = next(s for s in spans.get_finished_spans() if s.name == "service.tax.call")
    assert call.attributes["mock_io.anomaly"] == "slow_tax"


async def test_ground_truth_file_and_hidden_attribute(
    spans: InMemorySpanExporter, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "truth.jsonl"
    monkeypatch.setenv("MOCK_IO_GROUND_TRUTH_FILE", str(path))
    monkeypatch.setenv("MOCK_IO_ANOMALY_ATTRIBUTE", "0")
    group = _group(count=2, anomalies=[{"label": "spike", "latency_add_ms": 1}])
    await run_call_group(group, ZERO_DEFAULTS, _resources(), make_rng(1), "k")
    records = [json.loads(line) for line in path.read_text().splitlines()]
    finished = {s.context.span_id: s for s in spans.get_finished_spans() if s.name == "db.query"}
    assert [r["anomalies"] for r in records] == [["spike"], ["spike"]]
    for record in records:
        span = finished[int(record["span_id"], 16)]
        assert f"{span.context.trace_id:032x}" == record["trace_id"]
        assert "mock_io.anomaly" not in span.attributes
