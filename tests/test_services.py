import random

import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic import ValidationError

from mock_io_app.executor import IOResources, run_call_group
from mock_io_app.resources import FakeDatabase, FakeGCS, FakeHttpClient, FakeServiceClient
from mock_io_app.scenario import CallGroupSpec, Defaults, parse_scenario

SCENARIO = """
services:
  inner: {kind: first_party, call_groups: [{io: db, action: query}]}
  outer: {kind: first_party, call_groups: [{io: service, service: inner, action: call}]}
  vendor: {kind: third_party}
jobs:
  - name: j
    ops:
      - name: o
        call_groups: [{io: service, service: outer, action: call}]
"""


def _bad(service_yaml: str) -> str:
    return SCENARIO.replace("  vendor: {kind: third_party}", service_yaml)


def test_valid_scenario_parses() -> None:
    assert set(parse_scenario(SCENARIO).services) == {"inner", "outer", "vendor"}


@pytest.mark.parametrize(
    "service_yaml",
    [
        "  vendor: {kind: third_party, call_groups: [{io: db, action: query}]}",
        "  vendor: {kind: first_party}",
        "  inner2: {kind: first_party, call_groups: [{io: service, service: nope, action: call}]}",
    ],
)
def test_invalid_services_rejected(service_yaml: str) -> None:
    with pytest.raises(ValidationError):
        parse_scenario(_bad(service_yaml))


def test_cycle_rejected() -> None:
    text = SCENARIO.replace(
        "inner: {kind: first_party, call_groups: [{io: db, action: query}]}",
        "inner: {kind: first_party, call_groups: [{io: service, service: outer, action: call}]}",
    )
    with pytest.raises(ValidationError, match="cycle"):
        parse_scenario(text)


def test_service_io_requires_service_name() -> None:
    with pytest.raises(ValidationError):
        CallGroupSpec(io="service", action="call")
    with pytest.raises(ValidationError):
        CallGroupSpec(io="db", action="query", service="x")


async def test_first_party_traced_inside_third_party_black_box(
    spans: InMemorySpanExporter,
) -> None:
    scenario = parse_scenario(SCENARIO)
    resources = IOResources(
        db=FakeDatabase(latency_scale=0),
        gcs=FakeGCS(),
        http=FakeHttpClient(),
        services=FakeServiceClient(latency_scale=0),
    )
    defaults = Defaults(seed=1)
    for name in ("outer", "vendor"):
        group = CallGroupSpec(io="service", service=name, action="call")
        await run_call_group(group, defaults, resources, random.Random(1), "k", scenario.services)

    names = [s.name for s in spans.get_finished_spans()]
    assert "service.outer.handle" in names
    assert "service.inner.handle" in names
    assert "db.query" in names
    assert "service.vendor.call" in names
    assert "service.vendor.handle" not in names
