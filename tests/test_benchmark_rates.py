import argparse
import importlib.util
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parent.parent / "scripts" / "benchmark_rates.py"
spec = importlib.util.spec_from_file_location("benchmark_rates", SCRIPT)
assert spec and spec.loader
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


def test_parse_mem_usage_reads_docker_stats_units() -> None:
    assert bench.parse_mem_usage("123.5MiB / 7.6GiB") == pytest.approx(123.5 * 2**20)
    assert bench.parse_mem_usage("1.5GiB / 7.6GiB") == pytest.approx(1.5 * 2**30)
    with pytest.raises(ValueError):
        bench.parse_mem_usage("")


def test_parse_seconds() -> None:
    assert bench.parse_seconds("5s") == 5
    assert bench.parse_seconds("2m") == 120
    assert bench.parse_seconds("500ms") == 0.5
    with pytest.raises(argparse.ArgumentTypeError):
        bench.parse_seconds("soon")


def test_default_collectors_have_configs_and_start_with_reference() -> None:
    for name in bench.DEFAULT_COLLECTORS:
        assert (bench.ROOT / "collectors" / name / "config.yaml").exists()
    assert bench.DEFAULT_COLLECTORS[0] == bench.REFERENCE
