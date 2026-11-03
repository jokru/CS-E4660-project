"""Generate scenarios/supermarket.yaml: importers -> transformers -> loaders for a supermarket chain."""

import random
from pathlib import Path
from typing import Any

import yaml

OUTPUT = Path(__file__).resolve().parents[1] / "scenarios" / "supermarket.yaml"
SEED = 2026
BIG_FACTOR = 10

IMPORTERS = [
    "pos_sales", "loyalty_cards", "supplier_prices", "weather_feed", "shelf_sensors",
    "ecommerce_orders", "store_registry", "promotions", "warehouse_stock", "returns_log",
]  # fmt: skip
TRANSFORMERS = [
    "daily_basket_stats", "weekly_category_rollup", "margin_by_sku", "price_elasticity",
    "promo_uplift", "loyalty_segments", "demand_features", "shrinkage_estimates",
    "stock_cover_days", "supplier_scorecard", "store_cluster_profiles", "weather_adjusted_sales",
    "basket_affinity", "markdown_candidates", "ecommerce_funnel",
]  # fmt: skip
BIG_TRANSFORMERS = ["year_end_basket_rollup", "full_history_price_rebuild"]
LOADERS = [
    "store_dashboards", "replenishment_forecast", "markdown_recommendations", "supplier_portal",
    "loyalty_crm_sync", "finance_margin_report", "shrinkage_report", "promo_performance_board",
    "category_manager_pack", "regional_ops_summary", "ecommerce_merch_feed", "price_change_queue",
    "stock_alert_feed", "store_manager_digest", "executive_scorecard", "weather_sales_report",
    "delivery_planning_board", "returns_analysis_mart", "labor_scheduling_input",
    "customer_churn_mart",
]  # fmt: skip
BIG_LOADERS = ["full_year_data_warehouse_load", "historical_backfill_load"]

# (op name, action)
IMPORT_DB = [
    ("read_store_registry", "query"), ("lookup_sku_catalog", "query"),
    ("find_open_purchase_orders", "query"), ("select_loyalty_members", "query"),
    ("scan_price_history", "query"), ("check_import_watermark", "query"),
    ("resolve_supplier_codes", "query"), ("read_promotion_calendar", "query"),
    ("upsert_pos_transactions", "insert"), ("append_import_audit_log", "insert"),
    ("record_ingest_watermark", "insert"), ("insert_staging_receipts", "insert"),
    ("register_batch_manifest", "insert"), ("upsert_supplier_contacts", "insert"),
    ("store_shelf_snapshots", "insert"), ("log_rejected_rows", "insert"),
]  # fmt: skip
IMPORT_HTTP = [
    ("get_supplier_prices", "get"), ("get_weather_forecast", "get"),
    ("get_holiday_calendar", "get"), ("get_fx_rates", "get"), ("get_store_hours", "get"),
    ("get_courier_status", "get"), ("get_promo_feed", "get"), ("get_traffic_report", "get"),
    ("post_delivery_ack", "post"), ("post_import_heartbeat", "post"),
]  # fmt: skip
IMPORT_GCS = [
    ("download_raw_receipts", "download"), ("download_supplier_csv", "download"),
    ("download_sensor_dump", "download"), ("download_loyalty_export", "download"),
    ("download_promo_index", "download"), ("upload_staged_receipts", "upload"),
    ("upload_validated_batches", "upload"), ("upload_rejected_rows", "upload"),
    ("upload_normalized_events", "upload"), ("upload_import_manifest", "upload"),
]  # fmt: skip
TRANSFORM_GCS = [
    ("download_staged_sales", "download"), ("download_price_history", "download"),
    ("download_inventory_snapshots", "download"), ("download_loyalty_segments", "download"),
    ("download_weather_features", "download"), ("upload_basket_stats", "upload"),
    ("upload_margin_tables", "upload"), ("upload_demand_features", "upload"),
    ("upload_cleaned_receipts", "upload"), ("upload_category_rollups", "upload"),
]  # fmt: skip
LOAD_GCS = [
    ("download_category_rollups", "download"), ("download_margin_tables", "download"),
    ("download_demand_features", "download"), ("download_basket_stats", "download"),
    ("upload_dashboard_extracts", "upload"), ("upload_forecast_exports", "upload"),
    ("upload_load_manifest", "upload"), ("upload_report_pdfs", "upload"),
]  # fmt: skip
LOAD_DB = [
    ("read_dimension_keys", "query"), ("check_target_schema_version", "query"),
    ("select_stale_partitions", "query"), ("lookup_store_hierarchy", "query"),
    ("upsert_store_dashboard_rows", "insert"), ("merge_replenishment_forecast", "insert"),
    ("insert_markdown_recommendations", "insert"), ("upsert_shrinkage_report", "insert"),
    ("merge_loyalty_segments", "insert"), ("insert_load_audit_entry", "insert"),
]  # fmt: skip

PAYLOADS = [65_536, 262_144, 1_048_576]
HTTP_RETRIES = 3


def _names(rng: random.Random, pool: list[tuple[str, str]], n: int) -> list[tuple[str, str]]:
    """n distinct op names; pools are cycled with a numeric suffix when n exceeds their size."""
    shuffled = rng.sample(pool, len(pool))
    return [
        (name if i < len(pool) else f"{name}_{i // len(pool) + 1}", action)
        for i in range(n)
        for name, action in [shuffled[i % len(pool)]]
    ]


def _group(rng: random.Random, io: str, action: str) -> dict[str, Any]:
    group: dict[str, Any] = {"io": io, "action": action}
    if io == "db":
        count = rng.randint(5, 20) if action == "insert" and rng.random() < 0.4 else rng.randint(1, 4)
        group["count"] = count
        if count > 4:
            group.update(mode="gather", max_concurrency=4)
        group["latency"] = {"dist": "lognormal", "median_ms": 8, "sigma": 0.5}
    elif io == "http":
        count = rng.randint(1, 6)
        group["count"] = count
        if count > 1 and rng.random() < 0.5:
            group.update(mode="gather", max_concurrency=3)
        group["latency"] = {"dist": "normal", "mean_ms": 90, "stddev_ms": 25}
        group["failure_rate"] = 0.01
    else:
        count = rng.randint(1, 4)
        group["count"] = count
        if count > 1 and rng.random() < 0.4:
            group["mode"] = "gather"
        group["payload_bytes"] = rng.choice(PAYLOADS)
        group["latency"] = {"dist": "lognormal", "median_ms": 50, "sigma": 0.4}
    return group


def _ops(rng: random.Random, steps: list[tuple[str, str, str]]) -> list[dict[str, Any]]:
    ops = []
    for name, io, action in steps:
        op: dict[str, Any] = {"name": name, "call_groups": [_group(rng, io, action)]}
        if io == "http":
            op["retries"] = HTTP_RETRIES
        ops.append(op)
    return ops


def _steps(
    rng: random.Random, io: str, pool: list[tuple[str, str]], n: int
) -> list[tuple[str, str, str]]:
    return [(name, io, action) for name, action in _names(rng, pool, n)]


def _importer(rng: random.Random, name: str) -> dict[str, Any]:
    steps = (
        _steps(rng, "db", IMPORT_DB, rng.randint(5, 10))
        + _steps(rng, "http", IMPORT_HTTP, rng.randint(5, 10))
        + _steps(rng, "gcs", IMPORT_GCS, rng.randint(5, 10))
    )
    rng.shuffle(steps)
    return {"name": f"import_{name}", "ops": _ops(rng, steps)}


def _transformer(rng: random.Random, name: str, deps: list[str], factor: int) -> dict[str, Any]:
    steps = _steps(rng, "gcs", TRANSFORM_GCS, rng.randint(2, 5) * factor)
    return {"name": f"transform_{name}", "depends_on": deps, "ops": _ops(rng, steps)}


def _loader(rng: random.Random, name: str, deps: list[str], factor: int) -> dict[str, Any]:
    steps = _steps(rng, "gcs", LOAD_GCS, rng.randint(2, 5) * factor) + _steps(
        rng, "db", LOAD_DB, rng.randint(2, 5) * factor
    )
    rng.shuffle(steps)
    return {"name": f"load_{name}", "depends_on": deps, "ops": _ops(rng, steps)}


def build() -> dict[str, Any]:
    rng = random.Random(SEED)
    importers = [f"import_{n}" for n in IMPORTERS]
    jobs = [_importer(rng, n) for n in IMPORTERS]

    transformers = []
    for name in TRANSFORMERS + BIG_TRANSFORMERS:
        big = name in BIG_TRANSFORMERS
        deps = rng.sample(importers, rng.randint(3, 4) if big else rng.randint(1, 3))
        jobs.append(_transformer(rng, name, deps, BIG_FACTOR if big else 1))
        transformers.append(f"transform_{name}")

    for name in LOADERS + BIG_LOADERS:
        big = name in BIG_LOADERS
        # At least one transformer, plus a mix of any earlier tier.
        deps = rng.sample(transformers, 1)
        extra = [j for j in importers + transformers if j not in deps]
        deps += rng.sample(extra, rng.randint(0, 3 if big else 2))
        jobs.append(_loader(rng, name, deps, BIG_FACTOR if big else 1))

    return {
        "description": (
            "Supermarket chain data platform: 10 importers, 15+2 transformers, 20+2 loaders "
            "with mixed dependencies. Generated by scripts/generate_supermarket.py."
        ),
        "defaults": {
            "seed": 42,
            "latency": {"dist": "lognormal", "median_ms": 30, "sigma": 0.6},
            "failure_rate": 0.0,
        },
        "jobs": jobs,
    }


def render() -> str:
    return yaml.safe_dump(build(), sort_keys=False, width=120)


if __name__ == "__main__":
    OUTPUT.write_text(render())
