# SchemaOpt Environment

SchemaOpt is a standalone OpenEnv benchmark for workload-adaptive warehouse optimization.

## Scope

- Fixed analytical query workloads
- Clustered workload summaries plus deterministic query retrieval
- Logical derived objects: join materializations, aggregate materializations, filtered projections, denormalized tables
- Deterministic auto-routing from fixed queries to derived objects
- Correctness-gated performance reward

## Endpoints

- `POST /reset`
- `POST /step`
- `GET /state`
- `GET /schema`
- `GET /tasks`
- `GET /grader`
- `POST /baseline`
