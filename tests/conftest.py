from collections.abc import Iterator

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

# Global provider can only be set once per process, so install it before any test module imports.
_exporter = InMemorySpanExporter()
_provider = TracerProvider()
_provider.add_span_processor(SimpleSpanProcessor(_exporter))
trace.set_tracer_provider(_provider)


@pytest.fixture
def spans() -> Iterator[InMemorySpanExporter]:
    _exporter.clear()
    yield _exporter
    _exporter.clear()


def zero_latency_scenario(**overrides: object) -> dict:
    scenario = {
        "defaults": {"seed": 1, "latency": {"dist": "fixed", "ms": 0}},
        "jobs": [
            {
                "name": "alpha",
                "copies": 2,
                "ops": [
                    {
                        "name": "fetch",
                        "call_groups": [
                            {"io": "http", "action": "get", "count": 4, "mode": "gather"},
                            {"io": "db", "action": "query", "count": 2},
                        ],
                    },
                    {
                        "name": "store",
                        "call_groups": [{"io": "gcs", "action": "upload", "count": 3}],
                    },
                ],
            },
            {
                "name": "beta",
                "ops": [
                    {"name": "only", "call_groups": [{"io": "db", "action": "insert", "count": 1}]}
                ],
            },
        ],
    }
    scenario.update(overrides)
    return scenario
