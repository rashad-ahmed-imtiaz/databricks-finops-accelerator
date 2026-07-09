# Architecture

## Source System Tables

Required:

- `system.billing.usage`

Optional:

- `system.billing.list_prices`
- `system.compute.node_timeline`
- `system.lakeflow.jobs`
- `system.lakeflow.job_run_timeline`
- `system.lakeflow.job_task_run_timeline`

## Transformation Flow

```text
system.billing.usage
  -> daily_cost
  -> workload_cost_summary
  -> tagging_quality_summary
  -> job_reliability_summary failed-cost attribution

system.compute.node_timeline
  -> compute_utilization_summary

system.lakeflow.jobs + job_run_timeline + job_task_run_timeline
  -> job_reliability_summary

cost + utilization + reliability + tagging + attribution
  -> optimization_candidates
  -> accelerator_summary
  -> dashboard views
```

## Output Model

`daily_cost` is the core fact table. It includes workload metadata, tags, estimated cost, attribution quality, price source, and run ID.

`workload_cost_summary` aggregates spend and DBUs by workload.

`compute_utilization_summary` adds cluster-level utilization signals when `node_timeline` is available.

`job_reliability_summary` adds failure, cancellation, retry, duration, and estimated failed-cost signals when Lakeflow tables are available. Failed-cost attribution joins `system.billing.usage` directly by `usage_metadata.job_run_id`; it does not depend on `daily_cost` carrying job-run-level detail.

`tagging_quality_summary` tracks required tag coverage by date and workload.

`optimization_candidates` combines the model into a prioritized review backlog.

## Attribution Logic

Attribution quality is assigned as:

- `HIGH`: job, SQL warehouse, or pipeline metadata is available.
- `MEDIUM`: cluster or run-as metadata exists, but a stronger workload ID is unavailable.
- `LOW`: only broad cluster/product/SKU attribution is available, or shared cluster attribution is detected.
- `UNKNOWN`: useful workload metadata is missing.

Shared all-purpose cluster usage is marked with `SHARED_CLUSTER_ATTRIBUTION` because cost may not map cleanly to one owner or workload.

## Scoring Logic

`optimization_candidates.priority_score` is normalized to 0-100:

```text
priority_score =
  cost_score * 0.45
  + waste_score * 0.25
  + reliability_score * 0.15
  + tagging_score * 0.10
  + frequency_score * 0.05
```

The score intentionally favors cost impact while still accounting for utilization, reliability, tagging, attribution, and activity frequency.

The weights are defined once in `databricks_finops.scoring.SCORE_WEIGHTS`. The SQL expression used by `optimization_candidates` is generated from that Python constant so the Python unit logic and SQL model stay aligned.

## Deployment Targets

The bundle has two targets:

- `dev`: development mode, paused schedule, deployed under the current user's workspace bundle path.
- `prod`: production mode, unpaused schedule, deployed under the deployer workspace bundle path. In production, use a service principal CLI profile so ownership and permissions are stable.

Use `commands.ps1` as the canonical local command list and set `DATABRICKS_CONFIG_PROFILE` to the Databricks CLI profile name before running it. The script fails fast when the profile is missing.

## SQL Templates

Business SQL lives under `src/databricks_finops/sql/` and is included in the wheel package. Python modules load the templates with `importlib.resources`, substitute named placeholders, and submit the resulting SQL to Spark.

## Currency Handling

`currency_code` is the source currency for estimated cost. `display_currency` is written to output tables as the same value for dashboard convenience. The accelerator does not perform FX conversion, so mismatched display currencies are not accepted.

## Confidence Model

Confidence is:

- `HIGH`: strong utilization or reliability evidence exists.
- `MEDIUM`: partial optional source evidence exists.
- `LOW`: recommendation depends on fallback, missing optional sources, or insufficient data.

## Idempotency Strategy

Derived lookback-window tables use `CREATE OR REPLACE TABLE`, so reruns deterministically refresh the current lookback model.

`accelerator_run_log` appends one record per step and run.

`accelerator_health` is replaced each run with the latest health checks.

## Failure And Degradation Behavior

`system.billing.usage` is required and fails fast if unavailable.

Optional sources create WARN health rows and placeholder outputs with `INSUFFICIENT_DATA`, keeping downstream views queryable.
