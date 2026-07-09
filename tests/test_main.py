from __future__ import annotations

from importlib import resources
from pathlib import Path

import pytest
import yaml

import databricks_finops
from databricks_finops.config import (
    AppConfig,
    ISSUE_TYPES,
    OUTPUT_VIEWS,
    SUGGESTED_ACTIONS,
    TABLE_CONTRACTS,
    config_from_args,
)
from databricks_finops.costs import build_daily_cost_sql, build_workload_cost_summary_sql
from databricks_finops.main import parse_args
from databricks_finops.optimization import build_optimization_candidates_sql
from databricks_finops.preflight import PreflightResult, TableCapability
from databricks_finops.reliability import build_job_reliability_sql
from databricks_finops.run_logging import RUN_LOG_COLUMNS, StepLogger
from databricks_finops.scoring import (
    SCORE_WEIGHTS,
    priority_score_sql_expr,
    weighted_priority_score,
)
from databricks_finops.spark_utils import load_sql
from databricks_finops.summary import build_accelerator_summary_sql
from databricks_finops.tagging import build_tagging_quality_sql, tagging_quality_bucket
from databricks_finops.utilization import (
    build_compute_utilization_placeholder_sql,
    build_compute_utilization_sql,
)
from databricks_finops.views import build_view_sql_statements


ROOT = Path(__file__).resolve().parents[1]
TEST_CONFIG = AppConfig(
    catalog="finops",
    schema="accelerator",
    lookback_days=30,
    currency_code="USD",
    display_currency="USD",
    fallback_dbu_price=0.55,
    bundle_root=".",
)


def load_yaml(path: str) -> dict:
    return yaml.safe_load((ROOT / path).read_text(encoding="utf-8"))


def test_package_imports():
    assert databricks_finops.__version__ == "0.3.0"


def test_cli_parser_has_no_mode_argument():
    args = parse_args(
        [
            "--catalog",
            "finops",
            "--schema",
            "accelerator",
            "--lookback-days",
            "30",
            "--currency-code",
            "USD",
            "--fallback-dbu-price",
            "0.55",
            "--bundle-root",
            "/Workspace/example",
        ]
    )

    assert args.catalog == "finops"
    assert args.schema == "accelerator"
    assert args.lookback_days == 30
    assert not hasattr(args, "mode")
    assert config_from_args(args).display_currency == "USD"

    with pytest.raises(SystemExit):
        parse_args(["--mode", "anything"])

    with pytest.raises(SystemExit):
        parse_args(["--display-currency", "CAD"])


def test_databricks_yml_parses_and_has_real_defaults():
    bundle = load_yaml("databricks.yml")

    assert "mode" not in bundle["variables"]
    assert bundle["variables"]["catalog"]["default"] == "finops"
    assert bundle["variables"]["schema"]["default"] == "accelerator"

    dev_vars = bundle["targets"]["dev"]["variables"]
    assert dev_vars == {
        "catalog": "finops",
        "schema": "accelerator",
        "lookback_days": 30,
        "currency_code": "USD",
        "fallback_dbu_price": 0.55,
        "pause_status": "PAUSED",
    }
    prod_vars = bundle["targets"]["prod"]["variables"]
    assert prod_vars["pause_status"] == "UNPAUSED"
    assert bundle["targets"]["prod"]["mode"] == "production"
    assert bundle["targets"]["prod"]["workspace"]["root_path"].startswith("/Workspace/Users/")


def test_finops_resource_preserves_serverless_wheel_pattern():
    resources = load_yaml("resources/finops_job.yml")
    jobs = resources["resources"]["jobs"]
    assert set(jobs) == {"finops_accelerator_job"}

    job = jobs["finops_accelerator_job"]
    assert job["name"] == "[${bundle.target}] Databricks FinOps Accelerator"

    task = job["tasks"][0]
    assert task["task_key"] == "run_finops"
    assert task["environment_key"] == "default"
    assert "libraries" not in task
    assert task["python_wheel_task"]["package_name"] == "databricks_finops"
    assert task["python_wheel_task"]["entry_point"] == "main"
    assert "--mode" not in task["python_wheel_task"]["parameters"]

    environment = job["environments"][0]
    assert environment["environment_key"] == "default"
    assert environment["spec"]["environment_version"] == "2"
    assert environment["spec"]["dependencies"] == [
        "${workspace.artifact_path}/.internal/databricks_finops-0.3.0-py3-none-any.whl"
    ]


def test_readme_and_commands_keep_simple_workflow():
    readme = (ROOT / "README.md").read_text(encoding="utf-8").lower()
    commands = (ROOT / "commands.ps1").read_text(encoding="utf-8")
    old_profile = "ras" + "had"

    assert "sample mode" not in readme
    assert "mode=sample" not in readme
    assert old_profile not in readme
    assert "--var" not in commands
    assert "DATABRICKS_CONFIG_PROFILE" in commands
    assert "Set DATABRICKS_CONFIG_PROFILE" in commands
    assert f"-p {old_profile}" not in commands
    assert f'$profile = "{old_profile}"' not in commands
    assert "databricks bundle validate -t dev -p $profile" in commands
    assert "databricks bundle run finops_accelerator_job -t prod -p $profile" in commands


def test_table_contracts_include_required_fields():
    assert TABLE_CONTRACTS["daily_cost"] == [
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
    ]
    assert "priority_score" in TABLE_CONTRACTS["optimization_candidates"]
    assert "confidence" in TABLE_CONTRACTS["optimization_candidates"]
    assert "utilization_grain" in TABLE_CONTRACTS["compute_utilization_summary"]
    assert "job_run_id" not in TABLE_CONTRACTS["daily_cost"]


def test_issue_type_and_action_values_match_contract():
    assert {
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
    } == ISSUE_TYPES

    assert {
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
    } == SUGGESTED_ACTIONS


def test_scoring_outputs_are_zero_to_one_hundred():
    assert weighted_priority_score(0, 0, 0, 0, 0) == 0
    assert weighted_priority_score(100, 100, 100, 100, 100) == 100
    assert 0 <= weighted_priority_score(80, 70, 50, 20, 30) <= 100
    assert weighted_priority_score(200, -5, 50, 50, 50) <= 100


def test_score_weights_are_single_source_for_python_and_sql():
    assert sum(SCORE_WEIGHTS.values()) == pytest.approx(1.0)

    sql_expr = priority_score_sql_expr()
    for weight in SCORE_WEIGHTS.values():
        assert f"{weight:.2f}" in sql_expr

    optimization_sql = build_optimization_candidates_sql(TEST_CONFIG, "run-1")
    assert sql_expr in optimization_sql


def test_expected_views_are_declared():
    assert {
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
    } == set(OUTPUT_VIEWS)


def test_required_docs_exist():
    assert (ROOT / "docs" / "dashboard_guide.md").exists()
    assert (ROOT / "docs" / "architecture.md").exists()
    assert (ROOT / "docs" / "business_value.md").exists()


def test_reliability_run_cost_does_not_use_daily_cost_job_run_id():
    preflight = PreflightResult(
        capabilities=[
            TableCapability("system.billing.usage", "billing_usage_available", True, True, "", ""),
            TableCapability("system.billing.list_prices", "list_prices_available", False, True, "", ""),
            TableCapability("system.lakeflow.jobs", "lakeflow_jobs_available", False, True, "", ""),
            TableCapability(
                "system.lakeflow.job_run_timeline",
                "lakeflow_job_run_timeline_available",
                False,
                True,
                "",
                "",
            ),
            TableCapability(
                "system.lakeflow.job_task_run_timeline",
                "lakeflow_task_run_timeline_available",
                False,
                False,
                "",
                "",
            ),
        ],
        columns_by_table={},
    )

    sql = build_job_reliability_sql(TEST_CONFIG, "run-1", preflight)
    run_cost_sql = sql.split("run_cost AS", maxsplit=1)[1].split("joined AS", maxsplit=1)[0]

    assert "FROM system.billing.usage" in run_cost_sql
    assert "daily_cost" not in run_cost_sql
    assert "job_run_id AS run_id" in run_cost_sql


def test_non_required_step_logs_exception_class_name():
    logger = StepLogger("run-1")

    message = logger.run(
        "optional_step",
        lambda: (_ for _ in ()).throw(ValueError("bad optional branch")),
        required=False,
    )

    assert "ValueError" in message
    assert logger.records[-1].message.startswith("ValueError:")


def test_step_log_result_values():
    assert "result" in RUN_LOG_COLUMNS

    clean_logger = StepLogger("run-1")
    clean_logger.run("clean_step", lambda: "ok")
    assert clean_logger.records[-1].result == "SUCCESS"

    degraded_logger = StepLogger("run-2")
    degraded_logger.run(
        "optional_step",
        lambda: (_ for _ in ()).throw(RuntimeError("fallback path")),
        required=False,
    )
    assert degraded_logger.records[-1].result == "DEGRADED"

    failed_logger = StepLogger("run-3")
    with pytest.raises(RuntimeError):
        failed_logger.run(
            "required_step",
            lambda: (_ for _ in ()).throw(RuntimeError("critical path")),
        )
    assert failed_logger.records[-1].result == "FAILED"


def test_vw_tagging_quality_uses_semantic_worst_quality():
    sql = build_view_sql_statements(TEST_CONFIG)["vw_tagging_quality"]

    assert "MIN(tagging_quality)" not in sql
    assert "worst_tagging_quality" in sql
    assert "WHEN 'MISSING' THEN 0" in sql


def test_tagging_quality_thresholds():
    assert tagging_quality_bucket(0) == "GOOD"
    assert tagging_quality_bucket(1) == "PARTIAL"
    assert tagging_quality_bucket(2) == "PARTIAL"
    assert tagging_quality_bucket(3) == "POOR"
    assert tagging_quality_bucket(4) == "POOR"
    assert tagging_quality_bucket(5) == "MISSING"
    assert tagging_quality_bucket(0, workload_id="UNKNOWN") == "UNKNOWN"


def test_accelerator_summary_has_empty_lookback_sentinel():
    sql = build_accelerator_summary_sql(TEST_CONFIG, "run-1")

    assert "cost_rollup_source" in sql
    assert "UNION ALL" in sql
    assert "Sentinel row keeps downstream CROSS JOIN output stable" in sql
    assert "COALESCE(SUM(estimated_failed_cost), 0.0)" in sql


def test_sql_templates_are_packaged_and_renderable():
    expected_sql_files = {
        "daily_cost_list_prices.sql",
        "daily_cost_fallback.sql",
        "workload_cost_summary.sql",
        "compute_utilization_summary.sql",
        "compute_utilization_placeholder.sql",
        "job_reliability_summary.sql",
        "job_reliability_placeholder.sql",
        "tagging_quality_summary.sql",
        "optimization_candidates.sql",
        "accelerator_summary.sql",
    }
    expected_view_files = {f"{view}.sql" for view in OUTPUT_VIEWS}

    sql_root = resources.files("databricks_finops").joinpath("sql")
    package_sql_files = {path.name for path in sql_root.iterdir() if path.name.endswith(".sql")}
    package_view_files = {
        path.name for path in sql_root.joinpath("views").iterdir() if path.name.endswith(".sql")
    }

    assert expected_sql_files.issubset(package_sql_files)
    assert expected_view_files.issubset(package_view_files)

    rendered = load_sql(
        "daily_cost_fallback",
        target_table="target",
        latest_jobs_cte="",
        usage_projection="SELECT current_date() AS usage_date",
        fallback_price="0.55",
        job_join="",
        lookback_days="30",
        currency_code_sql="'USD'",
        display_currency_sql="'USD'",
        run_id_sql="'run-1'",
    )
    assert rendered.strip()
    assert "CREATE OR REPLACE TABLE" in rendered


def test_generated_sql_has_no_unresolved_placeholders_and_expected_targets():
    preflight = PreflightResult(
        capabilities=[
            TableCapability("system.billing.usage", "billing_usage_available", True, True, "", ""),
            TableCapability("system.billing.list_prices", "list_prices_available", False, True, "", ""),
            TableCapability("system.compute.node_timeline", "node_timeline_available", False, True, "", ""),
            TableCapability("system.lakeflow.jobs", "lakeflow_jobs_available", False, True, "", ""),
            TableCapability(
                "system.lakeflow.job_run_timeline",
                "lakeflow_job_run_timeline_available",
                False,
                True,
                "",
                "",
            ),
            TableCapability(
                "system.lakeflow.job_task_run_timeline",
                "lakeflow_task_run_timeline_available",
                False,
                True,
                "",
                "",
            ),
        ],
        columns_by_table={
            "system.billing.usage": {"custom_tags"},
            "system.compute.node_timeline": {
                "cpu_user_percent",
                "cpu_system_percent",
                "mem_used_percent",
                "network_sent_bytes",
                "network_received_bytes",
            },
            "system.lakeflow.job_task_run_timeline": {
                "workspace_id",
                "job_id",
                "run_id",
                "period_start_time",
                "task_run_id",
            },
        },
    )

    statements = [
        build_daily_cost_sql(TEST_CONFIG, "run-1", preflight, use_list_prices=True),
        build_daily_cost_sql(TEST_CONFIG, "run-1", preflight, use_list_prices=False),
        build_workload_cost_summary_sql(TEST_CONFIG, "run-1"),
        build_compute_utilization_sql(TEST_CONFIG, "run-1", preflight),
        build_compute_utilization_placeholder_sql(TEST_CONFIG, "run-1"),
        build_job_reliability_sql(TEST_CONFIG, "run-1", preflight),
        build_tagging_quality_sql(TEST_CONFIG, "run-1"),
        build_optimization_candidates_sql(TEST_CONFIG, "run-1"),
        build_accelerator_summary_sql(TEST_CONFIG, "run-1"),
        *build_view_sql_statements(TEST_CONFIG).values(),
    ]

    for statement in statements:
        assert "{" not in statement
        assert "}" not in statement
        assert "finops" in statement
        assert "accelerator" in statement
