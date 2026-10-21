import math
import random
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

MAX_LATENCY_MS = 60_000.0


class _Latency(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class FixedLatency(_Latency):
    dist: Literal["fixed"]
    ms: float = Field(default=0.0, ge=0, le=MAX_LATENCY_MS)

    def sample_ms(self, rng: random.Random) -> float:
        return self.ms


class UniformLatency(_Latency):
    dist: Literal["uniform"]
    min_ms: float = Field(ge=0, le=MAX_LATENCY_MS)
    max_ms: float = Field(ge=0, le=MAX_LATENCY_MS)

    @model_validator(mode="after")
    def _check_bounds(self) -> "UniformLatency":
        if self.min_ms > self.max_ms:
            raise ValueError("min_ms must be <= max_ms")
        return self

    def sample_ms(self, rng: random.Random) -> float:
        return rng.uniform(self.min_ms, self.max_ms)


class NormalLatency(_Latency):
    dist: Literal["normal"]
    mean_ms: float = Field(ge=0, le=MAX_LATENCY_MS)
    stddev_ms: float = Field(ge=0, le=MAX_LATENCY_MS)

    def sample_ms(self, rng: random.Random) -> float:
        return min(max(0.0, rng.gauss(self.mean_ms, self.stddev_ms)), MAX_LATENCY_MS)


class LogNormalLatency(_Latency):
    dist: Literal["lognormal"]
    median_ms: float = Field(gt=0, le=MAX_LATENCY_MS)
    sigma: float = Field(default=0.5, ge=0, le=5)

    def sample_ms(self, rng: random.Random) -> float:
        return min(rng.lognormvariate(math.log(self.median_ms), self.sigma), MAX_LATENCY_MS)


LatencySpec = Annotated[
    FixedLatency | UniformLatency | NormalLatency | LogNormalLatency,
    Field(discriminator="dist"),
]


def sample_seconds(latency: LatencySpec, rng: random.Random) -> float:
    return latency.sample_ms(rng) / 1000.0
