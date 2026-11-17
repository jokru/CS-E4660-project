from collections.abc import Iterator
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from mock_io_app.api.app import app, get_gateway, get_scenario_dir
from mock_io_app.api.gateway import GatewayError
from mock_io_app.scenario import MAX_SCENARIO_FILE_BYTES
from mock_io_app.tracing import TRACEPARENT_TAG
from tests.conftest import zero_latency_scenario

RUN_ID = "0b2c7d2e-5d1f-4b8e-9a51-1f0a3c4d5e6f"


class FakeGateway:
    def __init__(self) -> None:
        self.submitted: list[tuple[str, dict[str, str]]] = []
        self.reloads = 0
        self.fail_after: int | None = None

    def submit(self, job_name: str, tags: dict[str, str]) -> str:
        if self.fail_after is not None and len(self.submitted) >= self.fail_after:
            raise GatewayError("dagster down")
        self.submitted.append((job_name, dict(tags)))
        return f"00000000-0000-0000-0000-{len(self.submitted):012d}"

    def status(self, run_id: str) -> str:
        if run_id != RUN_ID:
            raise GatewayError("not found", status_code=404)
        return "SUCCESS"

    def reload(self) -> None:
        self.reloads += 1


@pytest.fixture
def gateway() -> FakeGateway:
    return FakeGateway()


@pytest.fixture
def client(tmp_path: Path, gateway: FakeGateway) -> Iterator[TestClient]:
    (tmp_path / "demo.yaml").write_text(yaml.safe_dump(zero_latency_scenario()))
    app.dependency_overrides[get_gateway] = lambda: gateway
    app.dependency_overrides[get_scenario_dir] = lambda: tmp_path
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()


def test_lists_scenarios_and_jobs(client: TestClient) -> None:
    scenarios = client.get("/scenarios").json()
    assert scenarios == [
        {
            "name": "demo",
            "description": "",
            "jobs": ["demo__alpha_0", "demo__alpha_1", "demo__beta"],
        }
    ]
    assert client.get("/jobs").json() == ["demo__alpha_0", "demo__alpha_1", "demo__beta"]


def test_launch_job_repeats(client: TestClient, gateway: FakeGateway) -> None:
    response = client.post("/jobs/demo__beta/runs", json={"repeat": 3, "tags": {"team": "perf"}})
    assert response.status_code == 200
    assert len(response.json()["runs"]) == 3
    assert [job for job, _ in gateway.submitted] == ["demo__beta"] * 3
    assert all(
        tags["team"] == "perf" and tags["mock_io/scenario"] == "demo"
        for _, tags in gateway.submitted
    )


def test_launch_job_without_body(client: TestClient, gateway: FakeGateway) -> None:
    assert client.post("/jobs/demo__beta/runs").status_code == 200
    assert len(gateway.submitted) == 1


def test_launch_unknown_job_is_404(client: TestClient) -> None:
    assert client.post("/jobs/demo__nope/runs", json={}).status_code == 404


def test_launch_rejects_reserved_tags(client: TestClient) -> None:
    response = client.post("/jobs/demo__beta/runs", json={"tags": {"otel/traceparent": "x"}})
    assert response.status_code == 422


def test_launch_scenario_runs_every_job(client: TestClient, gateway: FakeGateway) -> None:
    response = client.post("/scenarios/demo/runs", json={"repeat": 2})
    assert response.status_code == 200
    assert sorted(job for job, _ in gateway.submitted) == sorted(
        ["demo__alpha_0", "demo__alpha_1", "demo__beta"] * 2
    )


def test_partial_launch_failure_reports_launched_runs(
    client: TestClient, gateway: FakeGateway
) -> None:
    gateway.fail_after = 1
    response = client.post("/scenarios/demo/runs", json={})
    assert response.status_code == 502
    assert len(response.json()["detail"]["launched"]) == 1


def test_run_status(client: TestClient) -> None:
    assert client.get(f"/runs/{RUN_ID}").json() == {"run_id": RUN_ID, "status": "SUCCESS"}
    assert client.get("/runs/11111111-1111-1111-1111-111111111111").status_code == 404
    assert client.get("/runs/not-a-uuid").status_code == 422


def test_put_scenario_writes_and_reloads(
    client: TestClient, gateway: FakeGateway, tmp_path: Path
) -> None:
    body = yaml.safe_dump(zero_latency_scenario(description="new"))
    response = client.put("/scenarios/fresh", content=body)
    assert response.status_code == 201
    assert response.json()["jobs"] == ["fresh__alpha_0", "fresh__alpha_1", "fresh__beta"]
    assert (tmp_path / "fresh.yaml").read_text() == body
    assert gateway.reloads == 1
    assert client.put("/scenarios/fresh", content=body).status_code == 200
    assert not list(tmp_path.glob("*.tmp"))


@pytest.mark.parametrize(
    ("name", "body", "expected"),
    [
        ("Bad-Name", "{}", 422),
        ("a.b", "{}", 422),
        ("ok", "not a mapping", 422),
        ("ok", "jobs: [unclosed", 422),
        ("ok", "x: &a [1, 2]\ny: *a", 422),
        ("ok", "jobs: []", 422),
        ("ok", " " * (MAX_SCENARIO_FILE_BYTES + 1), 413),
    ],
)
def test_put_scenario_rejects_bad_input(
    client: TestClient, gateway: FakeGateway, tmp_path: Path, name: str, body: str, expected: int
) -> None:
    assert client.put(f"/scenarios/{name}", content=body).status_code == expected
    assert gateway.reloads == 0
    assert sorted(p.name for p in tmp_path.iterdir()) == ["demo.yaml"]


def test_traceparent_is_propagated_into_run_tags(client: TestClient, gateway: FakeGateway) -> None:
    trace_id = "4bf92f3577b34da6a3ce929d0e0e4736"
    headers = {"traceparent": f"00-{trace_id}-00f067aa0ba902b7-01"}
    assert client.post("/jobs/demo__beta/runs", json={}, headers=headers).status_code == 200
    traceparent = gateway.submitted[0][1][TRACEPARENT_TAG]
    assert traceparent.split("-")[1] == trace_id


def test_each_launched_run_has_a_run_span_in_the_request_trace(
    client: TestClient, gateway: FakeGateway, spans: InMemorySpanExporter
) -> None:
    trace_id = "4bf92f3577b34da6a3ce929d0e0e4736"
    headers = {"traceparent": f"00-{trace_id}-00f067aa0ba902b7-01"}
    response = client.post("/scenarios/demo/runs", json={"repeat": 2}, headers=headers)
    assert response.status_code == 200

    run_spans = [s for s in spans.get_finished_spans() if s.name.startswith("run.")]
    assert len(run_spans) == len(gateway.submitted) == 6  # (alpha_0, alpha_1, beta) x 2
    assert {f"{s.context.trace_id:032x}" for s in run_spans} == {trace_id}
    assert sorted(s.attributes["mock_io.iteration"] for s in run_spans) == [0, 0, 0, 1, 1, 1]
    # Each run's tags point at its own run span, which is what the job spans parent to.
    submitted_parents = [tags[TRACEPARENT_TAG].split("-")[2] for _, tags in gateway.submitted]
    assert sorted(submitted_parents) == sorted(f"{s.context.span_id:016x}" for s in run_spans)


def test_seed_is_passed_as_run_tag(client: TestClient, gateway: FakeGateway) -> None:
    assert client.post("/scenarios/demo/runs", json={"seed": 7}).status_code == 200
    assert {tags["mock_io/seed"] for _, tags in gateway.submitted} == {"7"}
    gateway.submitted.clear()
    assert client.post("/scenarios/demo/runs", json={}).status_code == 200
    assert all("mock_io/seed" not in tags for _, tags in gateway.submitted)
