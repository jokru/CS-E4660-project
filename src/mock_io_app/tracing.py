import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar

from opentelemetry import propagate, trace
from opentelemetry.context import Context
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import Span, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor,
)

TRACER_NAME = "mock_io_app"
TRACEPARENT_TAG = "otel/traceparent"
TRACESTATE_TAG = "otel/tracestate"

RUN_JOB_ATTRIBUTE = "mock_io.run_job"

_CARRIER_TO_TAG = {"traceparent": TRACEPARENT_TAG, "tracestate": TRACESTATE_TAG}

_configured = False
_processors: list[SpanProcessor] = []
_service_providers: dict[str, TracerProvider] = {}
_current_service: ContextVar[str | None] = ContextVar("mock_io_current_service", default=None)
_current_run_job: ContextVar[str | None] = ContextVar("mock_io_current_run_job", default=None)


class RunJobAttributeProcessor(SpanProcessor):
    """Tags every span with the job being run, so collectors can route spans per job."""

    def on_start(self, span: Span, parent_context: Context | None = None) -> None:
        job = _current_run_job.get()
        if job is not None:
            span.set_attribute(RUN_JOB_ATTRIBUTE, job)


@contextmanager
def for_run_job(name: str) -> Iterator[None]:
    token = _current_run_job.set(name)
    try:
        yield
    finally:
        _current_run_job.reset(token)


def get_tracer() -> trace.Tracer:
    service = _current_service.get()
    if service is not None and _processors:
        provider = _service_providers.get(service)
        if provider is None:
            provider = TracerProvider(resource=Resource.create({"service.name": service}))
            for processor in _processors:
                provider.add_span_processor(processor)
            _service_providers[service] = provider
        return provider.get_tracer(TRACER_NAME)
    return trace.get_tracer(TRACER_NAME)


def flush() -> None:
    for processor in _processors:
        processor.force_flush()


@contextmanager
def as_service(name: str | None) -> Iterator[None]:
    """Emit spans created inside this block under a different `service.name` (same exporter).

    `None` keeps the current service.
    """
    if name is None:
        yield
        return
    token = _current_service.set(name)
    try:
        yield
    finally:
        _current_service.reset(token)


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
        processor: SpanProcessor = SimpleSpanProcessor(ConsoleSpanExporter())
    elif kind == "otlp":
        processor = BatchSpanProcessor(_otlp_exporter())
    else:
        raise ValueError(f"unknown MOCK_IO_TRACE_EXPORTER {kind!r}")
    from mock_io_app.app_reducers import build_app_reducers

    reducers = build_app_reducers(os.environ.get("MOCK_IO_APP_REDUCERS", ""))
    _processors.extend([RunJobAttributeProcessor(), processor, *reducers])
    for added in _processors:
        provider.add_span_processor(added)
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
