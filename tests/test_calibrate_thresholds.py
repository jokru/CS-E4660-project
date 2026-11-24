import importlib.util
import json
from pathlib import Path

import yaml

from mock_io_app.app_reducers import Thresholds

SCRIPT = Path(__file__).parent.parent / "scripts" / "calibrate_thresholds.py"
spec = importlib.util.spec_from_file_location("calibrate_thresholds", SCRIPT)
assert spec and spec.loader
calibrate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(calibrate)

MS = 10**6


def _write_capture(path: Path) -> None:
    spans = []
    for i in range(30):
        start = i * 100_000 * MS
        run = 20_000 if i else 40_000  # one slower run
        spans += [
            {"name": "run.daily", "startTimeUnixNano": start, "endTimeUnixNano": start + run * MS},
            {"name": "op.x", "startTimeUnixNano": start, "endTimeUnixNano": start + 10 * MS},
            {"name": "db.query", "startTimeUnixNano": start, "endTimeUnixNano": start + 10 * MS},
        ]
    spans.append({"name": "rare", "startTimeUnixNano": 0, "endTimeUnixNano": 1})
    resource = {"attributes": [{"key": "service.name", "value": {"stringValue": "svc"}}]}
    line = {"resourceSpans": [{"resource": resource, "scopeSpans": [{"spans": spans}]}]}
    path.write_text(json.dumps(line) + "\n")


def test_calibration_writes_thresholds_and_collector_configs(tmp_path: Path) -> None:
    capture, out = tmp_path / "capture.jsonl", tmp_path / "thresholds.json"
    _write_capture(capture)
    args = ["--capture", str(capture), "--out", str(out), "--collectors-dir", str(tmp_path)]
    assert calibrate.main(args) == 0

    data = json.loads(out.read_text())
    # Skeleton and rare spans get no threshold; db.query gets factor x median.
    assert [(r["name"], r["threshold_ms"]) for r in data["spans"]] == [("db.query", 50.0)]
    [job] = data["jobs"]
    assert job["job"] == "daily" and job["latency_threshold_ms"] == 30_000
    assert job["decision_wait_s"] == 60  # 40 s x 1.25 + 5 s, rounded up to a bucket
    assert Thresholds.load(out).by_key == {("svc", "db.query"): 50 * MS}

    span_filter = yaml.safe_load((tmp_path / "span_filter" / "config.yaml").read_text())
    [condition] = span_filter["processors"]["filter/span_filter"]["trace_conditions"]
    assert 'span.name == "db.query"' in condition and 'Duration("50.0ms")' in condition

    routed = yaml.safe_load((tmp_path / "routed_tail_sampling" / "config.yaml").read_text())
    routing = routed["connectors"]["routing/routed_tail_sampling"]
    assert routing["table"][0]["pipelines"] == ["traces/routed_tail_sampling_60s"]
    assert routing["default_pipelines"] == ["traces/routed_tail_sampling_120s"]
    held = routed["processors"]["tail_sampling/routed_tail_sampling_60s"]
    assert held["decision_wait"] == "60s"
    assert "slow_daily" in [p["name"] for p in held["policies"]]
