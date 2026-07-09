from __future__ import annotations

from typing import Any

from .config import AppConfig
from .spark_utils import comment_on_table, load_sql, qname, sql_string


def create_tagging_quality_summary(spark: Any, config: AppConfig, run_id: str) -> str:
    spark.sql(build_tagging_quality_sql(config, run_id))
    return "tagging_quality_summary refreshed from daily_cost tags"


def tagging_quality_bucket(missing_required_tag_count: int, workload_id: str = "WORKLOAD") -> str:
    if workload_id == "UNKNOWN":
        return "UNKNOWN"
    if missing_required_tag_count == 0:
        return "GOOD"
    if missing_required_tag_count <= 2:
        return "PARTIAL"
    if missing_required_tag_count < 5:
        return "POOR"
    return "MISSING"


def build_tagging_quality_sql(config: AppConfig, run_id: str) -> str:
    return load_sql(
        "tagging_quality_summary",
        target_table=qname(config.catalog, config.schema, "tagging_quality_summary"),
        source_table=qname(config.catalog, config.schema, "daily_cost"),
        run_id_sql=sql_string(run_id),
    )


def comment_tagging_table(spark: Any, config: AppConfig) -> None:
    comment_on_table(
        spark,
        qname(config.catalog, config.schema, "tagging_quality_summary"),
        (
            "Daily tag coverage and missing-tag spend summary for project, team, owner, "
            "environment, and cost_center tags."
        ),
    )
