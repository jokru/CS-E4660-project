from pathlib import Path

import pytest
import yaml

ALL_CONFIGS = sorted((Path(__file__).parent.parent / "collectors").glob("*/config.yaml"))
# `jaeger` forwards traces for browsing and writes no output file.
CONFIGS = [c for c in ALL_CONFIGS if c.parent.name != "jaeger"]


@pytest.mark.parametrize("config", CONFIGS, ids=lambda p: p.parent.name)
def test_collector_writes_to_out_named_after_its_directory(config: Path) -> None:
    raw = yaml.safe_load(config.read_text())
    name = config.parent.name
    # Configs are merged when run together, so exporter and pipeline names must be unique.
    assert raw["exporters"][f"file/{name}"]["path"] == f"/out/{name}.jsonl"
    pipeline = raw["service"]["pipelines"][f"traces/{name}"]
    assert pipeline["exporters"] == [f"file/{name}"]
    assert set(pipeline["processors"]) <= set(raw["processors"])


def test_expected_collectors_exist() -> None:
    names = {c.parent.name for c in ALL_CONFIGS}
    assert {"baseline_naive", "sampling", "anomalies_sampling", "jaeger"} <= names


def test_jaeger_pipeline_name_is_unique_for_merging() -> None:
    raw = yaml.safe_load((CONFIGS[0].parent.parent / "jaeger" / "config.yaml").read_text())
    assert list(raw["service"]["pipelines"]) == ["traces/jaeger"]
