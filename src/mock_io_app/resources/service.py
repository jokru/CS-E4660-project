import asyncio
from collections.abc import Awaitable, Callable

from opentelemetry.trace import SpanKind
from opentelemetry.util.types import AttributeValue

from mock_io_app.ground_truth import anomaly_attributes, record_anomalies
from mock_io_app.resources.base import CallParams, FakeIOResource, MockIOError
from mock_io_app.tracing import as_service, get_tracer


class FakeServiceClient(FakeIOResource):
    async def call(
        self,
        service: str,
        params: CallParams,
        handler: Callable[[], Awaitable[object]] | None = None,
    ) -> None:
        """Call a service; `handler` (first-party only) runs inside a server span so its work is traced."""
        attributes: dict[str, AttributeValue] = {
            "mock_io.type": "service",
            "mock_io.action": "call",
            "mock_io.call_index": params.index,
            "mock_io.payload_bytes": params.payload_bytes,
            "mock_io.simulated_latency_ms": params.latency_s * self.latency_scale * 1000,
            "peer.service": service,
            "mock_io.service.party": "first" if handler else "third",
        }
        attributes.update(anomaly_attributes(params.anomalies))
        tracer = get_tracer()
        call_name = f"service.{service}.call"
        with tracer.start_as_current_span(
            call_name, kind=SpanKind.CLIENT, attributes=attributes
        ) as call_span:
            record_anomalies(call_span, call_name, params.anomalies, params.index)
            await asyncio.sleep(params.latency_s * self.latency_scale)
            if handler is not None:
                with (
                    as_service(service),
                    get_tracer().start_as_current_span(
                        f"service.{service}.handle",
                        kind=SpanKind.SERVER,
                        attributes={"mock_io.service": service},
                    ),
                ):
                    await handler()
            if params.fail:
                raise MockIOError(f"simulated service {service} failure (call {params.index})")
