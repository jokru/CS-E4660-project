from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from mock_io_app.scenario import (
    DEFAULT_SCENARIO_DIR,
    MAX_TOTAL_CALLS,
    Scenario,
    expand_jobs,
    load_scenarios,
)
from tests.conftest import zero_latency_scenario


def _job(name: str, copies: int = 1, ops: list | None = None) -> dict:
    return {
        "name": name,
        "copies": copies,
        "ops": ops
        or [{"name": "op", "call_groups": [{"io": "db", "action": "query", "count": 1}]}],
    }


@pytest.mark.parametrize("directory", [DEFAULT_SCENARIO_DIR, DEFAULT_SCENARIO_DIR / "stress"])
def test_bundled_scenarios_are_valid(directory: Path) -> None:
    assert load_scenarios(directory, "*.yaml")


def test_expand_jobs_applies_copies_and_prefix() -> None:
    scenario = Scenario.model_validate(zero_latency_scenario())
    names = [job.name for job in expand_jobs("demo", scenario)]
    assert names == ["demo__alpha_0", "demo__alpha_1", "demo__beta"]


def test_rejects_action_not_allowed_for_io() -> None:
    bad = {
        "jobs": [_job("a", ops=[{"name": "op", "call_groups": [{"io": "db", "action": "get"}]}])]
    }
    with pytest.raises(ValidationError, match="not allowed for io"):
        Scenario.model_validate(bad)


def test_rejects_names_colliding_after_expansion() -> None:
    with pytest.raises(ValidationError, match="collide"):
        Scenario.model_validate({"jobs": [_job("a", copies=2), _job("a_0")]})


def test_rejects_duplicate_op_names() -> None:
    op = {"name": "op", "call_groups": [{"io": "db", "action": "query"}]}
    with pytest.raises(ValidationError, match="duplicate op names"):
        Scenario.model_validate({"jobs": [_job("a", ops=[op, op])]})


def test_rejects_total_calls_over_cap() -> None:
    ops = [{"name": "op", "call_groups": [{"io": "db", "action": "query", "count": 10_000}]}]
    copies = MAX_TOTAL_CALLS // 10_000 + 1
    with pytest.raises(ValidationError, match="max is"):
        Scenario.model_validate({"jobs": [_job("a", copies=copies, ops=ops)]})


def test_rejects_unknown_fields_and_bad_latency() -> None:
    with pytest.raises(ValidationError):
        Scenario.model_validate({"jobs": [_job("a")], "unexpected": 1})
    with pytest.raises(ValidationError):
        Scenario.model_validate({"defaults": {"latency": {"ms": 5}}, "jobs": [_job("a")]})


def test_load_scenarios_rejects_bad_file_name(tmp_path: Path) -> None:
    (tmp_path / "Bad-Name.yaml").write_text(yaml.safe_dump(zero_latency_scenario()))
    with pytest.raises(ValueError, match="file name"):
        load_scenarios(tmp_path, "*.yaml")
