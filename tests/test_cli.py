from pathlib import Path

import yaml
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from mock_io_app.cli import main, reconstruct_job
from tests.conftest import zero_latency_scenario


def _scenario(tmp_path: Path) -> Path:
    raw = zero_latency_scenario()
    raw["jobs"][0]["executor"] = "multiprocess"
    path = tmp_path / "demo.yaml"
    path.write_text(yaml.safe_dump(raw))
    return path


def test_executor_option_overrides_the_scenario_executor(tmp_path: Path) -> None:
    path = str(_scenario(tmp_path))
    assert reconstruct_job(path, "demo__alpha_0", "in_process", 1.0).executor_def.name == (
        "in_process"
    )
    assert reconstruct_job(path, "demo__alpha_0", "multiprocess", 1.0).executor_def.name == (
        "multiprocess"
    )


def test_cli_runs_reconstructed_jobs_under_run_spans(
    tmp_path: Path, spans: InMemorySpanExporter
) -> None:
    path = _scenario(tmp_path)
    assert main(["--scenario", str(path), "--executor", "in_process", "--repeat", "2"]) == 0

    finished = spans.get_finished_spans()
    runs = [s for s in finished if s.name.startswith("run.")]
    jobs = [s for s in finished if s.name.startswith("job.")]
    assert len(runs) == 6  # alpha_0, alpha_1, beta x 2 iterations
    assert {j.parent.span_id for j in jobs} == {r.context.span_id for r in runs}
