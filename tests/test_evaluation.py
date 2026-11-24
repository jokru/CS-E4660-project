import json
from pathlib import Path

from mock_io_app.evaluation import (
    evaluate,
    format_report,
    label_traces,
    load_anomaly_spans,
    load_ground_truth,
    load_traces,
    main,
)


def _span(trace: str, span: str, start: int, end: int, error: bool = False) -> dict:
    return {
        "traceId": trace,
        "spanId": span,
        "startTimeUnixNano": str(start),
        "endTimeUnixNano": str(end),
        "status": {"code": "STATUS_CODE_ERROR" if error else "STATUS_CODE_UNSET"},
    }


def _write(path: Path, spans: list[dict]) -> None:
    # One export request per span, like the collector's file exporter with small batches.
    lines = [{"resourceSpans": [{"scopeSpans": [{"spans": [s]}]}]} for s in spans]
    path.write_text("\n".join(json.dumps(line) for line in lines) + "\n")


def _fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    reference = tmp_path / "reference.jsonl"
    # a: anomalous, b: error, c: slow, d and e: normal
    _write(
        reference,
        [
            _span("a", "a1", 0, 10),
            _span("a", "a2", 1, 5),
            _span("b", "b1", 0, 10, error=True),
            _span("c", "c1", 0, 1000),
            _span("d", "d1", 0, 10),
            _span("e", "e1", 0, 10),
        ],
    )
    output = tmp_path / "output.jsonl"
    _write(output, [_span("a", "a1", 0, 10), _span("c", "c1", 0, 1000), _span("d", "d1", 0, 10)])
    truth = tmp_path / "truth.jsonl"
    truth.write_text(json.dumps({"trace_id": "a", "anomalies": ["slow_tax_service"]}) + "\n")
    return reference, output, truth


def test_traces_are_labelled_from_ground_truth_status_and_duration(tmp_path: Path) -> None:
    reference, _, truth = _fixture(tmp_path)
    labels = label_traces(load_traces(reference), load_ground_truth(truth), slow_percentile=90)

    assert labels["a"].anomalies == {"slow_tax_service"}
    assert labels["b"].has_error and not labels["b"].anomalous
    assert [t for t, lab in labels.items() if lab.slow] == ["c"]


def test_evaluate_reports_reduction_and_recall(tmp_path: Path) -> None:
    reference, output, truth = _fixture(tmp_path)
    ref = load_traces(reference)
    labels = label_traces(ref, load_ground_truth(truth), slow_percentile=90)
    report = evaluate(ref, load_traces(output), labels, reference_bytes=100, output_bytes=40)

    assert report["reduction"]["traces_kept"] == 3 / 5
    assert report["reduction"]["spans_kept"] == 3 / 6
    assert report["reduction"]["bytes_kept"] == 0.4
    assert report["reduction"]["mean_trace_completeness"] == (0.5 + 1 + 1) / 3
    assert report["recall"]["anomalous"] == {"total": 1, "kept": 1, "recall": 1.0}
    assert report["recall"]["error"]["recall"] == 0.0
    assert report["recall"]["slow"]["recall"] == 1.0
    assert report["recall_by_anomaly"]["slow_tax_service"]["recall"] == 1.0
    assert report["precision"]["anomalous"] == 1 / 3
    assert "recall" in format_report(report)


def test_evidence_needs_an_injected_span_kept(tmp_path: Path) -> None:
    reference, _, _ = _fixture(tmp_path)
    truth = tmp_path / "spans_truth.jsonl"
    truth.write_text(json.dumps({"trace_id": "a", "span_id": "a2", "anomalies": ["x"]}) + "\n")
    ref = load_traces(reference)
    labels = label_traces(ref, load_ground_truth(truth))
    skeleton_only = tmp_path / "skeleton.jsonl"
    _write(skeleton_only, [_span("a", "a1", 0, 10)])
    with_span = tmp_path / "with_span.jsonl"
    _write(with_span, [_span("a", "a2", 1, 5)])

    for path, evidence in ((skeleton_only, 0.0), (with_span, 1.0)):
        report = evaluate(ref, load_traces(path), labels, anomaly_spans=load_anomaly_spans(truth))
        assert report["recall"]["anomalous"]["recall"] == 1.0
        assert report["evidence"]["recall"] == evidence


def test_cli_prints_json(tmp_path: Path, capsys) -> None:
    reference, output, truth = _fixture(tmp_path)
    args = ["--reference", str(reference), "--output", str(output), "--ground-truth", str(truth)]
    assert main([*args, "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["output"]["traces"] == 3


def test_cli_prints_matrix_for_several_outputs(tmp_path: Path, capsys) -> None:
    reference, output, truth = _fixture(tmp_path)
    nothing = tmp_path / "nothing.jsonl"
    _write(nothing, [_span("zzz", "z1", 0, 1)])
    base = ["--reference", str(reference), "--ground-truth", str(truth)]
    assert main([*base, "--output", str(output), "--output", str(nothing)]) == 0

    lines = capsys.readouterr().out.splitlines()
    assert lines[0].split()[:6] == ["method", "traces", "spans", "bytes", "evidence", "anomalous"]
    rows = {line.split()[0]: line.split() for line in lines[1:3]}
    assert rows["output"][1] == "60.0%"
    assert rows["nothing"][1] == "0.0%"
