"""Apply the app-side reducers (job_buffer, span_filter_path) to replayed traces.

The replay bypasses the app, so the reducers in `app_reducers.py` never see its spans. This module
reproduces their keep/drop rules on a recorded trace, so a replay can send the reduced stream
the app would have produced, at rates the app cannot reach. The rules are shared with the real
processors (`is_slow`-style threshold lookup, `is_sampled`, `SKELETON_PREFIXES`, the sample rate);
tests check both keep the same spans.

One approximation: the real processors know which parent links cross a process boundary
(`SpanContext.is_remote`). A capture does not record that, so a `job.*` span is treated as having
a remote parent (each job runs in its own process under the `run.*` span) and every other parent
link as local. On the seed-8 live capture this reproduces the real processors' output span for
span (31,920 spans for `span_filter_path`, 89,487 for `job_buffer`, of 220,354).
"""

import threading
from dataclasses import dataclass

from opentelemetry.proto.trace.v1.trace_pb2 import Status

from mock_io_app.app_reducers import (
    DEFAULT_SAMPLE_RATE,
    SKELETON_PREFIXES,
    Thresholds,
    is_sampled,
)

REDUCERS = ("job_buffer", "span_filter_path")
_ERROR = Status.STATUS_CODE_ERROR


@dataclass(frozen=True)
class _SpanInfo:
    span_id: bytes
    parent_id: bytes
    service: str
    name: str
    end_ns: int
    error: bool
    slow: bool
    threshold_known: bool


def _service_name(resource_spans) -> str:
    for attribute in resource_spans.resource.attributes:
        if attribute.key == "service.name":
            return attribute.value.string_value
    return ""


def _span_infos(template, thresholds: Thresholds) -> list[_SpanInfo]:
    infos = []
    for resource_spans in template.resource_spans:
        service = _service_name(resource_spans)
        for scope_spans in resource_spans.scope_spans:
            for span in scope_spans.spans:
                threshold = thresholds.by_key.get((service, span.name))
                duration = span.end_time_unix_nano - span.start_time_unix_nano
                infos.append(
                    _SpanInfo(
                        span_id=span.span_id,
                        parent_id=span.parent_span_id,
                        service=service,
                        name=span.name,
                        end_ns=span.end_time_unix_nano,
                        error=span.status.code == _ERROR,
                        slow=threshold is not None and duration >= threshold,
                        threshold_known=threshold is not None,
                    )
                )
    return infos


def _same_process(parent: _SpanInfo, child: _SpanInfo) -> bool:
    """A job runs in its own process, so a `job.*` span's parent link is remote."""
    return not child.name.startswith("job.")


def _local_parent(info: _SpanInfo, by_id: dict[bytes, _SpanInfo]) -> _SpanInfo | None:
    parent = by_id.get(info.parent_id) if info.parent_id else None
    return parent if parent is not None and _same_process(parent, info) else None


def span_filter_path_keep(infos: list[_SpanInfo]) -> set[bytes]:
    """Spans `SpanFilterPathProcessor` forwards: skeleton, errors, slow or uncalibrated spans, and
    the local ancestors of a forwarded detail span."""
    by_id = {i.span_id: i for i in infos}
    marked: set[bytes] = set()
    kept: set[bytes] = set()
    for info in sorted(infos, key=lambda i: i.end_ns):  # children end before their parents
        skeleton = info.name.startswith(SKELETON_PREFIXES)
        if not (
            info.span_id in marked
            or skeleton
            or info.error
            or not info.threshold_known
            or info.slow
        ):
            continue
        kept.add(info.span_id)
        parent = _local_parent(info, by_id)
        if parent is not None and not skeleton:
            marked.add(parent.span_id)
    return kept


def job_buffer_parts(infos: list[_SpanInfo]) -> list[tuple[frozenset[bytes], bool]]:
    """Local trace parts as `JobBufferProcessor` buffers them: (span ids, any error or slow)."""
    by_id = {i.span_id: i for i in infos}

    def root_of(info: _SpanInfo) -> bytes:
        while (parent := _local_parent(info, by_id)) is not None:
            info = parent
        return info.span_id

    members: dict[bytes, list[_SpanInfo]] = {}
    for info in infos:
        members.setdefault(root_of(info), []).append(info)
    return [
        (frozenset(i.span_id for i in part), any(i.error or i.slow for i in part))
        for part in members.values()
    ]


class Reducer:
    """Decides which spans of a replayed trace an app-side reducer would have forwarded."""

    def __init__(
        self, name: str, thresholds: Thresholds, sample_rate: float = DEFAULT_SAMPLE_RATE
    ) -> None:
        if name not in REDUCERS:
            raise ValueError(f"unknown reducer {name!r}, expected one of {REDUCERS}")
        self.name = name
        self.thresholds = thresholds
        self.sample_rate = sample_rate
        self._lock = threading.Lock()
        self._plans: dict[int, object] = {}

    def _plan(self, template):
        with self._lock:
            if id(template) not in self._plans:
                infos = _span_infos(template, self.thresholds)
                self._plans[id(template)] = (
                    span_filter_path_keep(infos)
                    if self.name == "span_filter_path"
                    else job_buffer_parts(infos)
                )
            return self._plans[id(template)]

    def kept_span_ids(self, template, trace_id: bytes) -> set[bytes]:
        """Original span IDs of `template` that survive, for a copy sent under `trace_id`."""
        plan = self._plan(template)
        if self.name == "span_filter_path":
            return set(plan)  # type: ignore[arg-type]
        sampled = is_sampled(int.from_bytes(trace_id, "big"), self.sample_rate)
        kept: set[bytes] = set()
        for ids, interesting in plan:  # type: ignore[misc]
            if interesting or sampled:
                kept |= ids
        return kept
