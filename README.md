---
title: SchemaOpt OpenEnv
emoji: 🧠
colorFrom: blue
colorTo: green
sdk: docker
pinned: false
app_port: 8000
base_path: /web
suggested_hardware: cpu-basic
tags:
  - openenv
  - fastapi
  - duckdb
  - benchmark
---

# SchemaOpt OpenEnv

SchemaOpt is an OpenEnv environment for workload-adaptive warehouse optimization. Agents do not rewrite workload SQL directly; they shape the physical layer by creating, modifying, and dropping derived objects that the router can use to accelerate real DuckDB workloads under storage, refresh, and step budgets.

## What This Environment Is

SchemaOpt models a practical warehouse-tuning loop rather than a single SQL-generation task. In each episode, the agent:

- inspects workload clusters and table statistics
- creates or adjusts derived objects such as aggregate materializations and denormalized tables
- benchmarks routed rewrites against the baseline workload
- submits a final solution before budgets are exhausted

The environment scores both correctness and performance. A rewrite only counts when the rewritten query returns the same results as the baseline query.

## Quick Start

### Run Locally

```bash
pip install -r requirements.txt
uvicorn server.app:app --host 0.0.0.0 --port 8000
```

### Smoke Test

```bash
curl http://localhost:8000/tasks
curl -X POST http://localhost:8000/reset -H "Content-Type: application/json" -d '{}'
```

### Run Inference

```bash
python inference.py --tasks schemaopt_easy_hiring_pipeline,schemaopt_medium_campaign_performance,schemaopt_hard_mobile_revenue_ops
```

## How It Runs

### Local Server

```bash
pip install -r requirements.txt
uvicorn server.app:app --host 0.0.0.0 --port 8000
```

### Local Inference Runner

```bash
python inference.py --task-id schemaopt_easy_hiring_pipeline --model-name gpt-5.4-mini
```

### Docker

```bash
docker build -t schemaopt-openenv .
docker run --rm -p 8000:8000 schemaopt-openenv
```

## Reset and Step

The environment follows the standard OpenEnv HTTP contract.

### Python Example

```python
from client import SchemaOptEnv
from models import SchemaOptAction

async def main():
    async with SchemaOptEnv(base_url="http://localhost:8000") as env:
        result = await env.reset(task_id="schemaopt_easy_hiring_pipeline")
        print(result.observation.decision_state)

        result = await env.step(
            SchemaOptAction(
                operation="get_cluster_context",
                cluster_id="schemaopt_easy_hiring_pipeline_cluster_03",
            )
        )
        print(result.observation.action_feedback)
```

### Synchronous Usage

```python
from client import SchemaOptEnv
from models import SchemaOptAction

with SchemaOptEnv(base_url="http://localhost:8000").sync() as env:
    result = env.reset(task_id="schemaopt_easy_hiring_pipeline")
    result = env.step(
        SchemaOptAction(
            operation="get_cluster_context",
            cluster_id="schemaopt_easy_hiring_pipeline_cluster_03",
        )
    )
    print(result.observation.decision_state)
```

### Typical Episode Flow

1. `reset(task_id=...)`
2. `get_cluster_context`
3. `create_derived_object` or `modify_derived_object`
4. `inspect_rewrite_status` or `benchmark_cluster`
5. Repeat until `submit` or the budget is exhausted.

Episodes auto-submit when the step budget runs out.

## Why It Exists

Schema optimization is a real platform problem: the strongest physical schema is often the one that speeds up repeated analytical queries without exhausting storage or refresh budgets. SchemaOpt turns that tradeoff into a deterministic benchmark with explicit actions, routing diagnostics, and holdout evaluation.

## Environment Highlights

- Real DuckDB execution on isolated per-episode database copies
- Typed action, observation, and state models
- Deterministic retrieval and rewrite diagnostics
- Correctness-gated benchmarking with final episode scoring
- 11 tasks across easy, medium, and hard difficulty

## Implementation Overview

- [inference.py](inference.py) is the submission runner. It prompts an OpenAI-compatible model for one JSON action per step, retries on parse failures, and drives a full episode while logging structured step output.
- [tasks.py](tasks.py) loads manifest-backed task definitions, canonicalizes query and cluster metadata, resolves runtime asset paths, and exposes helpers for task lookup and deterministic retrieval.
- [client.py](client.py) and [models.py](models.py) define the typed client and the action, observation, and state schemas shared by the runner and server.
- [server/schemaopt_environment.py](server/schemaopt_environment.py) is the core DuckDB-backed environment. It handles reset, step execution, derived-object creation and validation, rewrite evaluation, benchmarking, and final scoring.
- [server/app.py](server/app.py) exposes the FastAPI application, the standard OpenEnv routes, and benchmark-specific endpoints such as `/tasks`, `/baseline`, and `/grader`.
- [server/rubrics.py](server/rubrics.py) contains the reward tree that converts action feedback into step rewards and final submission scores.

## API Surface

- POST /reset
- POST /step
- GET /state
- GET /schema
- GET /tasks
- GET /grader
- POST /baseline

## Actions, Observations, and State

### Actions

Supported operations:

- inspect_catalog
- inspect_table_stats
- get_cluster_context
- inspect_rewrite_status
- create_derived_object
- modify_derived_object
- drop_derived_object
- benchmark_subset
- benchmark_cluster
- submit

Key validation rules:

- inspect_table_stats requires target_id
- get_cluster_context requires cluster_id
- inspect_rewrite_status requires exactly one scope: target_id, cluster_id, or query_ids
- create_derived_object and modify_derived_object require object_kind, name, sql_definition, source_objects
- drop_derived_object requires target_id
- benchmark_subset requires query_ids
- benchmark_cluster requires cluster_id

### Observations

Each observation includes:

- catalog summary
- workload summary
- retrieval context
- benchmark context
- router summary
- action feedback
- decision state

### State

State tracks:

- step count, difficulty, and task id
- remaining step, object, storage, and refresh budgets
- current focus cluster
- cluster attempt and benchmark history
- derived object usefulness and resource pressure

## Scoring

Step rewards come from the rubric tree:

- validation and runtime errors are penalized
- create and modify actions are rewarded when they help more workload clusters than they consume resources
- drop actions are rewarded when they remove empty, duplicate, or low-value objects
- benchmark actions pay out only on improvement, routing quality, and correctness
- submit returns the final score

Final score combines:

- 0.45 \* visible_gated_improvement
- 0.20 \* holdout_gated_improvement
- 0.20 \* correctness
- 0.10 \* migration_score
- 0.05 \* storage_score

## Tasks

Task manifests live in task_assets, with DuckDB databases under task_assets/databases.

SchemaOpt groups tasks by difficulty and workload family. Easy tasks favor clear aggregate reuse, medium tasks require broader cluster coverage, and hard tasks increase cross-cluster tradeoffs and penalize narrow objects.

### Easy Tasks

| Task ID                         | Workload                     | What it tests                                                                    |
| ------------------------------- | ---------------------------- | -------------------------------------------------------------------------------- |
| schemaopt_easy_geo_metrics      | Geographic metrics benchmark | Compact group-by reuse over country, city, and regional reporting.               |
| schemaopt_easy_hiring_pipeline  | Recruiting operations        | Funnel, requisition, and posting-health reporting with time-bucketed aggregates. |
| schemaopt_easy_product_adoption | Product adoption analytics   | Workspace usage, onboarding, and feature-adoption rollups.                       |
| schemaopt_easy_retail_ops       | Retail operations benchmark  | Small exact-match and filtered aggregate opportunities.                          |

### Medium Tasks

| Task ID                               | Workload                        | What it tests                                                      |
| ------------------------------------- | ------------------------------- | ------------------------------------------------------------------ |
| schemaopt_medium_campaign_performance | Paid acquisition analytics      | Reuse across portfolio, campaign, ad-group, and keyword slices.    |
| schemaopt_medium_customer_ops         | Customer operations benchmark   | Mixed aggregate and join-based reuse over service analytics.       |
| schemaopt_medium_delivery_operations  | Delivery operations analytics   | Portfolio, execution, backlog, and collaboration reporting.        |
| schemaopt_medium_motorsport_ops       | Motorsport operations benchmark | Clustered analytical queries with broader aggregate reuse choices. |

### Hard Tasks

| Task ID                             | Workload                               | What it tests                                                        |
| ----------------------------------- | -------------------------------------- | -------------------------------------------------------------------- |
| schemaopt_hard_lifecycle_engagement | Lifecycle marketing and deliverability | Campaign influence, engagement, churn, and deliverability tradeoffs. |
| schemaopt_hard_mobile_revenue_ops   | Mobile revenue operations              | Install, release, geography, platform, and monetization reporting.   |
| schemaopt_hard_sports_analytics     | Sports analytics benchmark             | Varied exact-match and grouped aggregate opportunities.              |

## Inference Runner

The submission runner is inference.py at the repository root. It sends one JSON action at a time to an OpenAI-compatible model, retries on malformed output, and logs structured [START], [STEP], and [END] records.

Common usage:

```bash
python inference.py --task-id schemaopt_easy_hiring_pipeline --model-name gpt-5.4-mini --max-steps 40 --max-action-retries 4
```

Environment variables:

- API_KEY
- API_BASE_URL
- MODEL_NAME
- MAX_STEPS
- TASK_ID
- MAX_ACTION_RETRIES
- ENV_URL
- HF_SPACE_REPO_ID or SPACE_ID

## Project Layout

```text
metaenv/
|- Dockerfile
|- client.py
|- inference.py
|- models.py
|- openenv.yaml
|- pyproject.toml
|- requirements.txt
|- tasks.py
|- server/
|  |- app.py
|  |- rubrics.py
|  |- schemaopt_environment.py
|- task_assets/
|  |- *.json
|  |- databases/*.duckdb
```

## Notes

- The environment can auto-submit when the step budget is exhausted.
- Large or low-utility derived objects increase resource pressure and reduce reward.
