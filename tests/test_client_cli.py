import json
from pathlib import Path

import httpx
import pytest

from mock_io_app.api.client_cli import main

RUN_ID = "0b2c7d2e-5d1f-4b8e-9a51-1f0a3c4d5e6f"


def _run(handler, *argv: str) -> int:
    return main(["--url", "http://api.test", *argv], transport=httpx.MockTransport(handler))


def test_run_job_posts_repeat_and_tags(capsys: pytest.CaptureFixture[str]) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"runs": [{"job": "demo__beta", "run_id": RUN_ID}]})

    assert _run(handler, "run-job", "demo__beta", "--repeat", "3", "--tag", "team=perf") == 0
    assert seen[0].url.path == "/jobs/demo__beta/runs"
    assert json.loads(seen[0].content) == {"repeat": 3, "tags": {"team": "perf"}}
    assert capsys.readouterr().out.strip() == f"demo__beta\t{RUN_ID}"


def test_wait_returns_nonzero_on_failed_run(capsys: pytest.CaptureFixture[str]) -> None:
    statuses = iter(["STARTED", "FAILURE"])

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json={"runs": [{"job": "j", "run_id": RUN_ID}]})
        return httpx.Response(200, json={"run_id": RUN_ID, "status": next(statuses)})

    code = _run(handler, "run-scenario", "demo", "--wait", "--poll-interval", "0")
    assert code == 1
    assert f"{RUN_ID}\tFAILURE" in capsys.readouterr().out


def test_api_error_is_reported(capsys: pytest.CaptureFixture[str]) -> None:
    code = _run(lambda r: httpx.Response(404, json={"detail": "job not found"}), "run-job", "x")
    assert code == 1
    assert "404" in capsys.readouterr().err


def test_unreachable_api_is_reported(capsys: pytest.CaptureFixture[str]) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused")

    assert _run(handler, "jobs") == 1
    assert "cannot reach API" in capsys.readouterr().err


def test_bad_tag_is_rejected(capsys: pytest.CaptureFixture[str]) -> None:
    assert _run(lambda r: httpx.Response(200, json={}), "run-job", "x", "--tag", "nokey") == 1
    assert "KEY=VALUE" in capsys.readouterr().err


def test_upload_sends_file(tmp_path: Path) -> None:
    path = tmp_path / "s.yaml"
    path.write_text('{"jobs": []}')
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(201, json={"name": "fresh", "description": "", "jobs": []})

    assert _run(handler, "upload", "fresh", str(path)) == 0
    assert seen[0].method == "PUT"
    assert seen[0].content == b'{"jobs": []}'
