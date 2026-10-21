import os
from collections.abc import Mapping

from opentelemetry import propagate, trace
from opentelemetry.context import Context
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor,
)

TRACER_NAME = "mock_io_app"
TRACEPARENT_TAG = "otel/traceparent"
TRACESTATE_TAG = "otel/tracestate"

_CARRIER_TO_TAG = {"traceparent": TRACEPARENT_TAG, "tracestate": TRACESTATE_TAG}

_configured = False


def get_tracer() -> trace.Tracer:
    return trace.get_tracer(TRACER_NAME)


def setup_tracing(service_name: str) -> None:
    """Configure the global tracer provider from MOCK_IO_TRACE_EXPORTER (none|console|otlp)."""
    global _configured
    if _configured:
        return
    _configured = True

    kind = os.environ.get("MOCK_IO_TRACE_EXPORTER", "none").lower()
    if kind == "none":
        return

    provider = TracerProvider(
        resource=Resource.create(
            {"service.name": os.environ.get("OTEL_SERVICE_NAME", service_name)}
        )
    )
    if kind == "console":
        provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
    elif kind == "otlp":
        provider.add_span_processor(BatchSpanProcessor(_otlp_exporter()))
    else:
        raise ValueError(f"unknown MOCK_IO_TRACE_EXPORTER {kind!r}")
    trace.set_tracer_provider(provider)


def _otlp_exporter():
    protocol = os.environ.get("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
    if protocol == "grpc":
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    elif protocol == "http/protobuf":
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    else:
        raise ValueError(f"unsupported OTEL_EXPORTER_OTLP_PROTOCOL {protocol!r}")
    return OTLPSpanExporter()


def trace_tags() -> dict[str, str]:
    """Current trace context as Dagster run tags."""
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return {tag: carrier[key] for key, tag in _CARRIER_TO_TAG.items() if key in carrier}


def context_from_tags(tags: Mapping[str, str]) -> Context:
    carrier = {key: tags[tag] for key, tag in _CARRIER_TO_TAG.items() if tag in tags}
    return propagate.extract(carrier)
