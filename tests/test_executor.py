import asyncio
from collections.abc import Mapping

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
from mock_io_app.scenario import CallGroupSpec, Defaults

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
