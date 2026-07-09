from __future__ import annotations

import argparse
import re
from dataclasses import dataclass


VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

ISSUE_TYPES = {
    "EXPENSIVE_WORKLOAD",
    "HEALTHY_EXPENSIVE",
    "HIGH_COST_LOW_UTILIZATION",
    "LOW_UTILIZATION",
    "MEMORY_BOUND",
    "HIGH_FAILURE_COST",
    "HIGH_FAILURE_RATE",
    "RETRY_HEAVY",
    "MISSING_TAGS",
    "UNKNOWN_ATTRIBUTION",
    "SHARED_CLUSTER_ATTRIBUTION",
    "INSUFFICIENT_DATA",
    "REVIEW_REQUIRED",
}

SUGGESTED_ACTIONS = {
    "REVIEW_WORKER_COUNT",
    "REVIEW_NODE_TYPE",
    "INVESTIGATE_MEMORY_PRESSURE",
    "INVESTIGATE_FAILURES",
    "INVESTIGATE_RETRIES",
    "IMPROVE_TAGGING",
    "REVIEW_ALL_PURPOSE_USAGE",
    "LOOKS_HEALTHY",
    "REVIEW_REQUIRED",
    "INSUFFICIENT_DATA",
}

TABLE_CONTRACTS = {
    "daily_cost": [
        "usage_date",
        "workspace_id",
        "billing_origin_product",
        "sku_name",
        "cloud",
        "usage_unit",
        "workload_type",
        "workload_id",
        "job_id",
        "job_name",
        "cluster_id",
        "warehouse_id",
        "pipeline_id",
        "notebook_id",
        "run_as",
        "project",
        "team",
        "owner",
        "environment",
        "cost_center",
        "dbus",
        "estimated_cost",
        "currency_code",
        "display_currency",
        "price_source",
        "attribution_quality",
        "attribution_notes",
        "created_at",
        "run_id",
    ],
    "workload_cost_summary": [
        "workspace_id",
        "workload_type",
        "workload_id",
        "job_id",
        "job_name",
        "cluster_id",
        "warehouse_id",
        "pipeline_id",
        "run_as",
        "project",
        "team",
        "owner",
        "environment",
        "cost_center",
        "total_dbus",
        "estimated_cost",
        "active_days",
        "first_seen_date",
        "last_seen_date",
        "avg_daily_cost",
        "cost_rank",
        "attribution_quality",
        "attribution_notes",
        "created_at",
        "run_id",
    ],
    "compute_utilization_summary": [
        "workspace_id",
        "cluster_id",
        "job_id",
        "job_name",
        "avg_cpu_pct",
        "peak_cpu_pct",
        "avg_memory_pct",
        "peak_memory_pct",
        "avg_network_sent_bytes",
        "avg_network_received_bytes",
        "dbus",
        "estimated_cost",
        "dbus_per_cpu_pct",
        "utilization_category",
        "utilization_grain",
        "sizing_signal",
        "confidence",
        "created_at",
        "run_id",
    ],
    "job_reliability_summary": [
        "workspace_id",
        "job_id",
        "job_name",
        "run_count",
        "success_count",
        "failed_count",
        "cancelled_count",
        "skipped_count",
        "retry_count",
        "failure_rate_pct",
        "avg_duration_minutes",
        "total_duration_minutes",
        "estimated_failed_dbus",
        "estimated_failed_cost",
        "last_run_start_time",
        "last_run_result_state",
        "reliability_category",
        "confidence",
        "created_at",
        "run_id",
    ],
    "tagging_quality_summary": [
        "usage_date",
        "workspace_id",
        "workload_type",
        "workload_id",
        "run_as",
        "dbus",
        "estimated_cost",
        "project",
        "team",
        "owner",
        "environment",
        "cost_center",
        "missing_project_tag",
        "missing_team_tag",
        "missing_owner_tag",
        "missing_environment_tag",
        "missing_cost_center_tag",
        "missing_required_tag_count",
        "missing_critical_tag_count",
        "tagging_quality",
        "created_at",
        "run_id",
    ],
    "optimization_candidates": [
        "priority_rank",
        "workspace_id",
        "workload_type",
        "workload_id",
        "job_id",
        "job_name",
        "owner",
        "team",
        "project",
        "environment",
        "monthly_dbus",
        "estimated_monthly_cost",
        "avg_cpu_pct",
        "avg_memory_pct",
        "failure_rate_pct",
        "retry_count",
        "issue_type",
        "suggested_action",
        "waste_score",
        "frequency_score",
        "reliability_score",
        "tagging_score",
        "priority_score",
        "confidence",
        "evidence",
        "created_at",
        "run_id",
    ],
}

OUTPUT_TABLES = [
    *TABLE_CONTRACTS.keys(),
    "accelerator_summary",
    "accelerator_health",
    "accelerator_run_log",
]

OUTPUT_VIEWS = [
    "vw_executive_summary",
    "vw_cost_trend_daily",
    "vw_most_expensive_workloads",
    "vw_optimization_backlog",
    "vw_tagging_quality",
    "vw_architecture_review_candidates",
    "vw_system_table_coverage",
    "vw_attribution_quality",
    "vw_compute_sizing_signals",
    "vw_failed_jobs",
    "vw_wasteful_workloads",
]


@dataclass(frozen=True)
class AppConfig:
    catalog: str
    schema: str
    lookback_days: int
    currency_code: str
    display_currency: str
    fallback_dbu_price: float
    bundle_root: str


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Databricks FinOps Accelerator using real Databricks System Tables."
    )
    parser.add_argument("--catalog", default="finops")
    parser.add_argument("--schema", default="accelerator")
    parser.add_argument("--lookback-days", type=int, default=30)
    parser.add_argument("--currency-code", default="USD")
    parser.add_argument("--fallback-dbu-price", type=float, default=0.55)
    parser.add_argument("--bundle-root", default=".")
    return parser.parse_args(argv)


def validate_identifier(value: str, label: str) -> str:
    if not VALID_IDENTIFIER.match(value):
        raise ValueError(
            f"Invalid {label}: {value}. Use a simple Unity Catalog identifier such as finops."
        )
    return value


def validate_currency(value: str, label: str) -> str:
    normalized = value.upper()
    if not re.match(r"^[A-Z]{3}$", normalized):
        raise ValueError(f"Invalid {label}: {value}. Use a three-letter code such as USD.")
    return normalized


def config_from_args(args: argparse.Namespace) -> AppConfig:
    return AppConfig(
        catalog=validate_identifier(args.catalog, "catalog"),
        schema=validate_identifier(args.schema, "schema"),
        lookback_days=max(1, args.lookback_days),
        currency_code=validate_currency(args.currency_code, "currency code"),
        display_currency=validate_currency(args.currency_code, "currency code"),
        fallback_dbu_price=max(0.0, args.fallback_dbu_price),
        bundle_root=args.bundle_root,
    )
