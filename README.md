# Databricks FinOps Accelerator

The Databricks FinOps Accelerator is a bundle-first, serverless Databricks job that builds a dashboard-ready FinOps layer from real Databricks System Tables.

It helps answer:

- Where is Databricks spend going?
- Which teams, projects, owners, and workloads drive spend?
- Which spend is untagged, unattributed, or low confidence?
- Which workloads should be reviewed first?
- What is the estimated cost impact of failures, retries, and inefficient compute?
- Which optional System Tables are unavailable, and which outputs are affected?

## Workflow

```powershell
$env:DATABRICKS_CONFIG_PROFILE = "your-profile"
$profile = $env:DATABRICKS_CONFIG_PROFILE

databricks bundle validate -t dev -p $profile
databricks bundle deploy -t dev -p $profile
databricks bundle run finops_accelerator_job -t dev -p $profile

databricks bundle validate -t prod -p $profile
databricks bundle deploy -t prod -p $profile
databricks bundle run finops_accelerator_job -t prod -p $profile
```

All defaults live in `databricks.yml`. The `dev` target deploys paused under the current user's workspace path. The `prod` target deploys unpaused under the deployer workspace path, which should normally be a CI service principal profile.

## Architecture

```text
Databricks System Tables
  system.billing.usage                 required
  system.billing.list_prices           optional
  system.compute.node_timeline         optional
  system.lakeflow.jobs                 optional
  system.lakeflow.job_run_timeline     optional
  system.lakeflow.job_task_run_timeline optional

        |
        v

Serverless Databricks Asset Bundle job
  Poetry build -> Python wheel -> python_wheel_task
  environment_key: default
  environment_version: "2"
  no task-level libraries
  no classic cluster

        |
        v

finops.accelerator Delta model
  daily_cost -> summaries -> optimization backlog -> dashboard views
```

## Defaults

- `catalog`: `finops`
- `schema`: `accelerator`
- `lookback_days`: `30`
- `currency_code`: `USD`
- `fallback_dbu_price`: `0.55`
- `pause_status`: `PAUSED` in `dev`, `UNPAUSED` in `prod`

## Prerequisites

Required:

- Databricks CLI configured with a profile exported as `DATABRICKS_CONFIG_PROFILE`
- Serverless jobs enabled
- Unity Catalog access to create or use `finops.accelerator`
- Read access to `system.billing.usage`

Recommended optional sources:

- `system.billing.list_prices` for list-price based estimated cost
- `system.compute.node_timeline` for compute utilization signals
- `system.lakeflow.jobs` for job names
- `system.lakeflow.job_run_timeline` for reliability signals
- `system.lakeflow.job_task_run_timeline` for retry signals

If optional tables are unavailable, the job continues, writes WARN records to `accelerator_health`, and creates valid downstream tables with `INSUFFICIENT_DATA` where needed.

## Output Tables

- `finops.accelerator.daily_cost`
- `finops.accelerator.workload_cost_summary`
- `finops.accelerator.compute_utilization_summary`
- `finops.accelerator.job_reliability_summary`
- `finops.accelerator.tagging_quality_summary`
- `finops.accelerator.optimization_candidates`
- `finops.accelerator.accelerator_summary`
- `finops.accelerator.accelerator_health`
- `finops.accelerator.accelerator_run_log`

## Dashboard Views

Business views:

- `finops.accelerator.vw_executive_summary`
- `finops.accelerator.vw_cost_trend_daily`
- `finops.accelerator.vw_most_expensive_workloads`
- `finops.accelerator.vw_optimization_backlog`
- `finops.accelerator.vw_tagging_quality`

Platform and architecture views:

- `finops.accelerator.vw_architecture_review_candidates`
- `finops.accelerator.vw_system_table_coverage`
- `finops.accelerator.vw_attribution_quality`
- `finops.accelerator.vw_compute_sizing_signals`
- `finops.accelerator.vw_failed_jobs`
- `finops.accelerator.vw_wasteful_workloads`

## Validation SQL

```sql
SHOW TABLES IN finops.accelerator;

SELECT *
FROM finops.accelerator.accelerator_health
ORDER BY created_at DESC;

SELECT *
FROM finops.accelerator.accelerator_summary
ORDER BY created_at DESC;

SELECT *
FROM finops.accelerator.vw_optimization_backlog
ORDER BY priority_rank;
```

## Local Checks

```powershell
poetry run pytest
poetry build
```

## Limitations

- `estimated_cost` is an estimate from Databricks list prices or the configured fallback DBU price. It is not an exact invoice.
- `display_currency` in output tables is a read-through alias of `currency_code`; no FX conversion is performed.
- Utilization is cluster-level when sourced from `system.compute.node_timeline`; it may not perfectly map to a single job.
- Serverless attribution depends on available billing metadata.
- Optional System Tables may not be enabled in every workspace.
- Low CPU does not automatically mean waste. Memory pressure, reliability, business criticality, and workload design must be reviewed before action.
- Suggested actions are review prompts only. The accelerator does not perform automatic remediation.

## Troubleshooting

- If the job fails at preflight, check access to `system.billing.usage`.
- If prices use `FALLBACK_DBU_PRICE`, check access to `system.billing.list_prices` and the selected currency.
- If utilization is `INSUFFICIENT_DATA`, check whether `system.compute.node_timeline` is enabled.
- If reliability is `INSUFFICIENT_DATA`, check Lakeflow job system tables.
- If catalog creation warns, ensure the target catalog exists or grant the job principal permission to create it.

More detail:

- `docs/dashboard_guide.md`
- `docs/architecture.md`
- `docs/business_value.md`
