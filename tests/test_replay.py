import json
import random
from concurrent import futures
from pathlib import Path

import grpc
import pytest
from opentelemetry.proto.collector.trace.v1 import trace_service_pb2, trace_service_pb2_grpc

from mock_io_app.evaluation import load_ground_truth
from mock_io_app.replay import instantiate, load_capture, main, replay

TRACE = "0af7651916cd43dd8448eb211c80319c"
ROOT, CHILD = "b7ad6b7169203331", "00f067aa0ba902b7"


def _span(span_id: str, parent: str, start: int, end: int, **extra) -> dict:
    span = {"traceId": TRACE, "spanId": span_id, "name": "op", "kind": 1}
    span |= {"startTimeUnixNano": str(start), "endTimeUnixNano": str(end), **extra}
    return span | ({"parentSpanId": parent} if parent else {})


def _write_capture(tmp_path: Path) -> tuple[Path, Path]:
    capture, truth = tmp_path / "capture.jsonl", tmp_path / "truth.jsonl"
    resource = {"attributes": [{"key": "service.name", "value": {"stringValue": "svc"}}]}
    spans = [
        _span(ROOT, "", 1_000, 5_000),
        _span(CHILD, ROOT, 2_000, 4_000, status={"code": 2}),
    ]
    # Spans of one trace split across lines, as the file exporter writes batches.
    lines = [
        {"resourceSpans": [{"resource": resource, "scopeSpans": [{"scope": {}, "spans": [s]}]}]}
        for s in spans
    ]
    capture.write_text("\n".join(json.dumps(line) for line in lines) + "\n")
    record = {"trace_id": TRACE, "span_id": CHILD, "anomalies": ["slow_x"], "timestamp": 1.0}
    truth.write_text(json.dumps(record) + "\n")
    return capture, truth


def test_instantiate_gives_fresh_consistent_ids_and_shifts_time(tmp_path: Path) -> None:
    [template] = load_capture(*_write_capture(tmp_path))
    assert template.span_count == 2 and template.end_ns == 5_000

    copies, records = instantiate(template, now_ns=1_000_000, rng=random.Random(1))
    spans = {
        s.name + str(s.start_time_unix_nano): s for c in copies for s in c.scope_spans[0].spans
    }
    root, child = spans["op996000"], spans["op997000"]

    assert root.trace_id == child.trace_id != bytes.fromhex(TRACE)
    assert child.parent_span_id == root.span_id != bytes.fromhex(ROOT)
    assert not root.parent_span_id
    assert (child.end_time_unix_nano, root.end_time_unix_nano) == (999_000, 1_000_000)
    assert child.status.code == 2
    assert records == [
        {
            "trace_id": child.trace_id.hex(),
            "span_id": child.span_id.hex(),
            "anomalies": ["slow_x"],
            "timestamp": pytest.approx(1.0 + 995_000 / 1e9),
        }
    ]
    # The template is unchanged, so every copy starts from the recording.
    assert template.resource_spans[0].scope_spans[0].spans[0].trace_id == bytes.fromhex(TRACE)


def test_replay_cycles_batches_and_skips_ground_truth_of_failed_requests(tmp_path: Path) -> None:
    templates = load_capture(*_write_capture(tmp_path))
    sent: list[int] = []

    def export(request: trace_service_pb2.ExportTraceServiceRequest) -> None:
        sent.append(
            len({s.trace_id for r in request.resource_spans for s in r.scope_spans[0].spans})
        )
        if len(sent) == 2:
            raise RuntimeError("collector refused")

    out = tmp_path / "out.jsonl"
    with out.open("w") as handle:
        stats = replay(
            templates, export, traces=5, rate=None, workers=1, batch=2, ground_truth_out=handle
        )

    assert sorted(sent) == [1, 2, 2]
    assert (stats.traces, stats.spans, stats.requests) == (5, 10, 3)
    assert (stats.failed_requests, stats.failed_spans) == (1, 4)
    assert dict(stats.errors) == {"RuntimeError": 1}
    assert len(load_ground_truth(out)) == 3


def test_main_sends_over_grpc(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    received: list[trace_service_pb2.ExportTraceServiceRequest] = []

    class Collector(trace_service_pb2_grpc.TraceServiceServicer):
        def Export(self, request, context):  # noqa: N802 - gRPC method name
            received.append(request)
            return trace_service_pb2.ExportTraceServiceResponse()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    trace_service_pb2_grpc.add_TraceServiceServicer_to_server(Collector(), server)
    try:
        port = server.add_insecure_port("127.0.0.1:0")
    except RuntimeError:
        pytest.skip("cannot bind a local port here")
    server.start()
    capture, truth = _write_capture(tmp_path)
    try:
        code = main(
            [
                "--capture", str(capture),
                "--ground-truth", str(truth),
                "--ground-truth-out", str(tmp_path / "replayed.jsonl"),
                "--traces", "3",
                "--endpoint", f"http://127.0.0.1:{port}",
            ]
        )  # fmt: skip
    finally:
        server.stop(None)

    assert code == 0
    assert len(received) == 3
    assert len(load_ground_truth(tmp_path / "replayed.jsonl")) == 3
    assert "sent 3 traces, 6 spans" in capsys.readouterr().out


def test_seeded_replays_send_the_same_trace_ids(tmp_path: Path) -> None:
    templates = load_capture(*_write_capture(tmp_path))

    def trace_ids(seed: int | None) -> set[bytes]:
        seen: set[bytes] = set()

        def export(request: trace_service_pb2.ExportTraceServiceRequest) -> None:
            seen.update(s.trace_id for r in request.resource_spans for s in r.scope_spans[0].spans)

        replay(templates, export, traces=4, rate=None, workers=3, seed=seed)
        return seen

    assert len(trace_ids(7)) == 4
    assert trace_ids(7) == trace_ids(7) != trace_ids(8)
    assert trace_ids(None) != trace_ids(None)
