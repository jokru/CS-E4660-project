import random

import pytest
from pydantic import TypeAdapter, ValidationError

from mock_io_app.latency import LatencySpec, sample_seconds

_adapter = TypeAdapter(LatencySpec)


@pytest.mark.parametrize(
    "spec",
    [
        {"dist": "fixed", "ms": 5},
        {"dist": "uniform", "min_ms": 1, "max_ms": 10},
        {"dist": "normal", "mean_ms": 1, "stddev_ms": 50},
        {"dist": "lognormal", "median_ms": 10, "sigma": 1.0},
    ],
)
def test_samples_are_seeded_and_non_negative(spec: dict) -> None:
    latency = _adapter.validate_python(spec)
    rng_a, rng_b = random.Random(3), random.Random(3)
    a = [sample_seconds(latency, rng_a) for _ in range(200)]
    b = [sample_seconds(latency, rng_b) for _ in range(200)]
    assert a == b
    assert all(s >= 0 for s in a)


def test_uniform_respects_bounds() -> None:
    latency = _adapter.validate_python({"dist": "uniform", "min_ms": 10, "max_ms": 20})
    rng = random.Random(0)
    assert all(0.010 <= sample_seconds(latency, rng) <= 0.020 for _ in range(500))


def test_uniform_rejects_inverted_bounds() -> None:
    with pytest.raises(ValidationError):
        _adapter.validate_python({"dist": "uniform", "min_ms": 20, "max_ms": 10})
