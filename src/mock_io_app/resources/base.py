import asyncio
from collections.abc import Mapping
from dataclasses import dataclass

from dagster import ConfigurableResource
from opentelemetry.trace import SpanKind
from opentelemetry.util.types import AttributeValue
from pydantic import Field

from mock_io_app.tracing import get_tracer


class MockIOError(Exception):
    pass


@dataclass(frozen=True)
class CallParams:
    latency_s: float
    fail: bool
    payload_bytes: int
    index: int


class FakeIOResource(ConfigurableResource):
    latency_scale: float = Field(default=1.0, ge=0)

    async def _simulate(
        self,
        io_type: str,
        action: str,
        params: CallParams,
        attributes: Mapping[str, AttributeValue] | None = None,
    ) -> None:
        span_attributes: dict[str, AttributeValue] = {
            "mock_io.type": io_type,
            "mock_io.action": action,
            "mock_io.call_index": params.index,
            "mock_io.payload_bytes": params.payload_bytes,
            "mock_io.simulated_latency_ms": params.latency_s * self.latency_scale * 1000,
            **(attributes or {}),
        }
        with get_tracer().start_as_current_span(
            f"{io_type}.{action}", kind=SpanKind.CLIENT, attributes=span_attributes
        ):
            await asyncio.sleep(params.latency_s * self.latency_scale)
            if params.fail:
                raise MockIOError(f"simulated {io_type}.{action} failure (call {params.index})")
