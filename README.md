---
title: SchemaOpt OpenEnv
sdk: docker
app_port: 8000
suggested_hardware: cpu-basic
tags:
- openenv
- fastapi
- duckdb
- benchmark
---

# SchemaOpt OpenEnv

SchemaOpt is an OpenEnv benchmark for workload-adaptive warehouse optimization. Agents operate over real DuckDB-backed analytical workloads and must design reusable derived objects that improve routed query performance while preserving exact correctness.

## Why this environment

Schema optimization is a real data-platform task: engineers and automated advisors routinely decide whether to materialize joins, aggregates, and denormalized tables to improve dashboard and reporting performance under storage and maintenance constraints. SchemaOpt turns that process into a grounded agent benchmark with explicit actions, correctness-gated rewrites, and holdout generalization.

## Environment shape

- Real DuckDB execution on isolated per-episode database copies
- Typed OpenEnv action, observation, and state models
- Dense reward shaping for object utility, benchmark progress, and cleanup quality
- Final unified grader over visible improvement, holdout improvement, correctness, migration quality, and storage efficiency
- Eleven tasks spanning easy, medium, and hard workloads

## Action space

The environment currently supports:

- `inspect_catalog`
- `inspect_table_stats`
- `get_cluster_context`
- `inspect_rewrite_status`
- `create_derived_object`
- `modify_derived_object`
- `drop_derived_object`
- `benchmark_subset`
- `benchmark_cluster`
- `submit`

## Observation and state

Observations include:

- catalog summary
- workload summary
- benchmark context
- router summary
- action feedback
- decision state

The state tracks:

- step count and difficulty
- remaining step and object budget
- remaining storage and refresh budget
- current focus cluster
- cluster attempt and benchmark history
- useful vs unused derived objects

## Tasks

The suite currently includes curated warehouse tasks plus benchmark-style tasks, with at least easy, medium, and hard coverage. Each task has a deterministic grader and task-local workload/database assets under `schemaopt_env/task_assets/`.

## Local run

Install dependencies:

```bash
pip install -r requirements.txt
```

Start the API server:

```bash
uvicorn schemaopt_env.server.app:app --host 0.0.0.0 --port 8000
```

Smoke test:

```bash
curl http://localhost:8000/tasks
curl -X POST http://localhost:8000/reset -H "Content-Type: application/json" -d '{}'
```

## Inference

The submission runner is at repo root:

```bash
python inference.py --tasks schemaopt_easy_hiring_pipeline,schemaopt_medium_campaign_performance,schemaopt_hard_mobile_revenue_ops
```

Environment variables:

- `OPENAI_API_KEY`
- `API_BASE_URL`
- `MODEL_NAME`
- `HF_TOKEN`

## Docker

Build:

```bash
docker build -t schemaopt-openenv .
```

Run:

```bash
docker run --rm -p 8000:8000 schemaopt-openenv
```
