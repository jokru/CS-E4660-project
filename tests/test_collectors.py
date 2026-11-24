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
    # Configs are merged when run together, so exporter, connector and pipeline names must be
    # unique: they all carry the collector's name.
    assert raw["exporters"][f"file/{name}"]["path"] == f"/out/{name}.jsonl"
    pipelines = raw["service"]["pipelines"]
    assert f"traces/{name}" in pipelines
    assert all(p.startswith(f"traces/{name}") for p in pipelines)
    assert all(c.endswith(f"/{name}") for c in raw.get("connectors") or {})
    connectors = set(raw.get("connectors") or {})
    exported = set()
    for pipeline in pipelines.values():
        assert set(pipeline["processors"]) <= set(raw["processors"])
        assert set(pipeline["receivers"]) <= set(raw["receivers"]) | connectors
        assert set(pipeline["exporters"]) <= set(raw["exporters"]) | connectors
        exported |= set(pipeline["exporters"])
    assert f"file/{name}" in exported


@pytest.mark.parametrize("section", ["receivers", "processors"])
def test_shared_names_have_identical_settings(section: str) -> None:
    # A merged config keeps one definition per name, so differing ones would clash.
    seen: dict[str, tuple[str, object]] = {}
    for config in ALL_CONFIGS:
        for name, settings in (yaml.safe_load(config.read_text()).get(section) or {}).items():
            first = seen.setdefault(name, (config.parent.name, settings))
            assert first[1] == settings, f"{name} differs in {first[0]} and {config.parent.name}"


def test_expected_collectors_exist() -> None:
    names = {c.parent.name for c in ALL_CONFIGS}
    assert {"baseline_naive", "sampling", "anomalies_sampling", "jaeger"} <= names


def test_jaeger_pipeline_name_is_unique_for_merging() -> None:
    raw = yaml.safe_load((CONFIGS[0].parent.parent / "jaeger" / "config.yaml").read_text())
    assert list(raw["service"]["pipelines"]) == ["traces/jaeger"]
