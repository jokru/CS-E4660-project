import importlib.util
from collections import Counter
from pathlib import Path

import pytest

from mock_io_app.scenario import DEFAULT_SCENARIO_DIR, expand_jobs, load_scenario

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "generate_supermarket.py"
_SPEC = importlib.util.spec_from_file_location("generate_supermarket", _SCRIPT)
assert _SPEC and _SPEC.loader
generator = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(generator)

SCENARIO_PATH = DEFAULT_SCENARIO_DIR / "supermarket.yaml"


def _by_tier():
    jobs = load_scenario(SCENARIO_PATH).jobs
    return {t: [j for j in jobs if j.name.startswith(f"{t}_")] for t in ("import", "transform", "load")}


def _steps(job, io):
    return sum(1 for op in job.ops for g in op.call_groups if g.io == io)


def test_committed_yaml_matches_generator() -> None:
    assert SCENARIO_PATH.read_text() == generator.render()


def test_tier_sizes() -> None:
    tiers = _by_tier()
    assert {t: len(j) for t, j in tiers.items()} == {"import": 10, "transform": 17, "load": 22}


def test_importers_are_independent_with_5_to_10_steps_per_io() -> None:
    for job in _by_tier()["import"]:
        assert job.depends_on == []
        assert all(5 <= _steps(job, io) <= 10 for io in ("db", "http", "gcs"))


def test_transformers_and_loaders_step_ranges() -> None:
    tiers = _by_tier()
    big = {*generator.BIG_TRANSFORMERS, *generator.BIG_LOADERS}
    for job in tiers["transform"] + tiers["load"]:
        factor = 10 if job.name.split("_", 1)[1] in big else 1
        io_ranges = {"gcs": (2, 5)} if job.name.startswith("transform_") else {
            "gcs": (2, 5),
            "db": (2, 5),
        }
        for io, (lo, hi) in io_ranges.items():
            assert lo * factor <= _steps(job, io) <= hi * factor, job.name
        if job.name.startswith("transform_"):
            assert _steps(job, "db") == 0 and _steps(job, "http") == 0


def test_dependencies_follow_tiers() -> None:
    tiers = _by_tier()
    for job in tiers["transform"]:
        assert job.depends_on and all(d.startswith("import_") for d in job.depends_on)
    for job in tiers["load"]:
        assert any(d.startswith("transform_") for d in job.depends_on)
    combos = Counter(tuple(sorted(j.depends_on)) for j in tiers["transform"] + tiers["load"])
    assert len(combos) > 20


def test_transformers_and_loaders_call_services() -> None:
    scenario = load_scenario(SCENARIO_PATH)
    tiers = _by_tier()
    kinds = set()
    for job in tiers["transform"] + tiers["load"]:
        called = [g.service for op in job.ops for g in op.call_groups if g.io == "service"]
        assert called, job.name
        kinds.update(scenario.services[s].kind for s in called)
    assert kinds == {"first_party", "third_party"}
    assert all(_steps(job, "service") == 0 for job in tiers["import"])


def test_expands() -> None:
    assert len(expand_jobs("supermarket", load_scenario(SCENARIO_PATH))) == 49


@pytest.mark.parametrize("name", ["transform_year_end_basket_rollup", "load_historical_backfill_load"])
def test_big_jobs_exist(name: str) -> None:
    assert name in {j.name for j in load_scenario(SCENARIO_PATH).jobs}
