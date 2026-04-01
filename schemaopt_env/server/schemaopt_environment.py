"""Core schema optimization environment implementation."""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
import math
import re
from typing import Any, Dict, Generic, List, Optional, Sequence, TypeVar
from uuid import uuid4

try:
    from ..models import SchemaOptAction, SchemaOptObservation, SchemaOptState
    from ..tasks import (
        TASK_CATALOG,
        ClusterSpec,
        QuerySpec,
        TaskSpec,
        cluster_lookup,
        get_task,
        match_queries,
        query_lookup,
        similar_query_ids,
        visible_queries_for_cluster,
    )
except ImportError:
    from models import SchemaOptAction, SchemaOptObservation, SchemaOptState
    from tasks import (
        TASK_CATALOG,
        ClusterSpec,
        QuerySpec,
        TaskSpec,
        cluster_lookup,
        get_task,
        match_queries,
        query_lookup,
        similar_query_ids,
        visible_queries_for_cluster,
    )


def _extract_select_aliases(sql: str) -> List[str]:
    match = re.search(r"select\s+(.*?)\s+from\s", sql, re.IGNORECASE | re.DOTALL)
    if not match:
        return []
    select_clause = match.group(1)
    parts = re.split(r",(?![^()]*\))", select_clause)
    aliases: List[str] = []
    for part in parts:
        cleaned = part.strip()
        alias_match = re.search(r"\bas\s+([A-Za-z_][A-Za-z0-9_]*)\s*$", cleaned, re.IGNORECASE)
        if alias_match:
            aliases.append(alias_match.group(1).strip().lower())
        else:
            aliases.append(cleaned.split(".")[-1].split()[-1].strip('"').lower())
    return aliases


try:
    import duckdb
except ImportError:
    class _FakeDuckDBConnection:
        def __init__(self):
            self.tables: Dict[str, Dict[str, Any]] = {}
            self.description = None
            self._last_fetchone = (0,)

        def execute(self, sql: str):
            normalized = " ".join(sql.strip().split())
            lowered = normalized.lower()
            if lowered.startswith("create schema"):
                self.description = None
                return self
            if lowered.startswith("drop table if exists"):
                name = normalized.split()[-1].lower()
                self.tables.pop(name, None)
                self.description = None
                return self
            if lowered.startswith("create table") and " as (" not in lowered:
                name = normalized.split()[2].lower()
                columns_section = normalized[normalized.find("(") + 1 : normalized.rfind(")")]
                columns = [part.strip().split()[0].strip('"').lower() for part in columns_section.split(",")]
                self.tables[name] = {"columns": columns, "rows": []}
                self.description = None
                return self
            if lowered.startswith("create table") and " as (" in lowered:
                name = normalized.split()[2].lower()
                body = sql[sql.lower().find("as (") + 4 :].strip()
                if body.startswith("("):
                    body = body[1:]
                if body.endswith(")"):
                    body = body[:-1]
                columns = _extract_select_aliases(body) or ["col1"]
                rows = [tuple(None for _ in columns) for _ in range(3)]
                self.tables[name] = {"columns": columns, "rows": rows}
                self.description = [(column, None, None, None, None, None, None) for column in columns]
                self._last_fetchone = (len(rows),)
                return self
            if lowered.startswith("select count(*) from"):
                name = normalized.split()[-1].lower()
                row_count = len(self.tables.get(name, {}).get("rows", []))
                self._last_fetchone = (row_count,)
                self.description = [("count", None, None, None, None, None, None)]
                return self
            if lowered.startswith("select * from") and "limit 0" in lowered:
                name = normalized.split()[3].lower()
                columns = self.tables.get(name, {}).get("columns", [])
                self.description = [(column, None, None, None, None, None, None) for column in columns]
                return self
            raise ValueError(f"Unsupported SQL in local fallback connection: {sql}")

        def executemany(self, sql: str, rows: Sequence[Sequence[Any]]):
            normalized = " ".join(sql.strip().split())
            table_name = normalized.split()[2].lower()
            entry = self.tables.setdefault(table_name, {"columns": [], "rows": []})
            entry["rows"].extend(list(rows))
            return self

        def fetchone(self):
            return self._last_fetchone

        def close(self):
            return None

    class _DuckDBModule:
        DuckDBPyConnection = _FakeDuckDBConnection

        @staticmethod
        def connect(database: str = ":memory:") -> _FakeDuckDBConnection:
            return _FakeDuckDBConnection()

    duckdb = _DuckDBModule()

try:
    from openenv.core.env_server.interfaces import Environment
    from openenv.core.env_server.types import State
except ImportError:
    try:
        from openenv_core.env_server.interfaces import Environment
        from openenv_core.env_server.types import State
    except ImportError:
        _A = TypeVar('_A')
        _O = TypeVar('_O')
        _S = TypeVar('_S')

        class Environment(Generic[_A, _O, _S]):
            pass

        State = SchemaOptState

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass
class DerivedObject:
    """Derived logical structure created by the agent."""

    name: str
    object_kind: str
    sql_definition: str
    source_objects: List[str]
    grain_dims: List[str]
    intended_clusters: List[str]
    routing_tags: List[str]
    available_columns: List[str]
    row_count: int
    storage_multiplier: float
    refresh_cost: float
    validation_error: Optional[str] = None
    used_by_visible_queries: set[str] = field(default_factory=set)
    used_by_clusters: set[str] = field(default_factory=set)

    @property
    def qualified_name(self) -> str:
        return f"derived.{self.name}"

    @property
    def filter_tags(self) -> set[str]:
        return {tag for tag in self.routing_tags if "=" in tag or tag.startswith("window=")}

    @property
    def feature_tags(self) -> set[str]:
        return {tag for tag in self.routing_tags if tag not in self.filter_tags}

    def summary(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "object_kind": self.object_kind,
            "source_objects": list(self.source_objects),
            "grain_dims": list(self.grain_dims),
            "intended_clusters": list(self.intended_clusters),
            "routing_tags": list(self.routing_tags),
            "available_columns": list(self.available_columns),
            "row_count": self.row_count,
            "storage_multiplier": self.storage_multiplier,
            "refresh_cost": self.refresh_cost,
            "used_by_visible_queries": sorted(self.used_by_visible_queries),
            "used_by_clusters": sorted(self.used_by_clusters),
            "validation_error": self.validation_error,
        }


@dataclass
class RouteDecision:
    """Route evaluation for a single query."""

    query_id: str
    routed: bool
    object_name: Optional[str]
    runtime_ms: float
    plan_depth: int
    operator_count: int
    join_complexity: int
    blocking_complexity: int
    correctness_pass: bool
    semantic_score: float
    route_reason: str

    def summary(self) -> Dict[str, Any]:
        return {
            "query_id": self.query_id,
            "routed": self.routed,
            "object_name": self.object_name,
            "runtime_ms": self.runtime_ms,
            "plan_depth": self.plan_depth,
            "operator_count": self.operator_count,
            "join_complexity": self.join_complexity,
            "blocking_complexity": self.blocking_complexity,
            "correctness_pass": self.correctness_pass,
            "semantic_score": self.semantic_score,
            "route_reason": self.route_reason,
        }


class SchemaOptEnvironment(Environment[SchemaOptAction, SchemaOptObservation, SchemaOptState]):
    """Standalone OpenEnv benchmark for workload-adaptive schema optimization."""

    SUPPORTS_CONCURRENT_SESSIONS: bool = True
    LAST_GRADER_REPORT: Dict[str, Any] = {
        "available": False,
        "reason": "No episode executed yet.",
    }

    def __init__(self):
        self._state = SchemaOptState(episode_id=str(uuid4()), step_count=0)
        self._task_catalog = TASK_CATALOG
        self._task: TaskSpec = get_task("schemaopt_easy_lever")
        self._visible_query_lookup: Dict[str, QuerySpec] = {}
        self._all_query_lookup: Dict[str, QuerySpec] = {}
        self._cluster_lookup: Dict[str, ClusterSpec] = {}
        self._derived_objects: Dict[str, DerivedObject] = {}
        self._checkpoints: List[Dict[str, Any]] = []
        self._retrieval_context: Dict[str, Any] = {}
        self._benchmark_context: Dict[str, Any] = {}
        self._last_error_signature: Optional[str] = None
        self._last_feedback: Dict[str, Any] = {}
        self._latest_visible_benchmark: Dict[str, Any] = {"gated_improvement": 0.0, "correctness_coverage": 1.0}
        self._last_failed_object_name: Optional[str] = None
        self._con: Optional[duckdb.DuckDBPyConnection] = None

    def reset(
        self,
        seed: Optional[int] = None,
        episode_id: Optional[str] = None,
        task_id: Optional[str] = None,
        **kwargs: Any,
    ) -> SchemaOptObservation:
        selected_task_id = task_id or kwargs.get("task_id") or "schemaopt_easy_lever"
        self._task = get_task(selected_task_id)
        self._visible_query_lookup = {query.query_id: query for query in self._task.visible_queries}
        self._all_query_lookup = query_lookup(self._task)
        self._cluster_lookup = cluster_lookup(self._task)
        self._derived_objects = {}
        self._checkpoints = []
        self._retrieval_context = {"last_request": None, "matched_queries": [], "matched_clusters": [], "retrieval_count": 0}
        self._benchmark_context = {
            "baseline_weighted_cost": self._task.total_visible_weighted_cost,
            "current_weighted_cost": self._task.total_visible_weighted_cost,
            "raw_improvement": 0.0,
            "gated_improvement": 0.0,
            "correctness_coverage": 1.0,
            "routed_query_count": 0,
            "incorrect_query_count": 0,
            "last_benchmarked_query_ids": [],
            "last_benchmarked_cluster_id": None,
            "latest_plan_deltas": {},
        }
        self._latest_visible_benchmark = {"gated_improvement": 0.0, "correctness_coverage": 1.0}
        self._last_feedback = {"event": "reset", "task_id": selected_task_id}
        self._last_error_signature = None
        self._last_failed_object_name = None
        self._seed_connection()

        self._state = SchemaOptState(
            episode_id=str(uuid4()) if episode_id is None else episode_id,
            step_count=0,
            done=False,
            task_id=self._task.task_id,
            difficulty=self._task.difficulty,
            derived_object_count=0,
            checkpoint_count=0,
            retrieval_count=0,
            benchmark_runs=0,
            storage_used_multiplier=0.0,
            final_score=None,
            last_error=None,
        )

        payload = self._task.reset_payload()
        return SchemaOptObservation(
            status="ok",
            message=f"Initialized task {self._task.task_id}",
            catalog_summary=payload["catalog_summary"],
            workload_summary=payload["workload_summary"],
            retrieval_context=self._retrieval_context,
            benchmark_context=self._benchmark_context,
            action_feedback=self._last_feedback,
            reward=0.0,
            done=False,
            metadata={"task": payload["task"]},
        )

    def step(
        self,
        action: SchemaOptAction,
        timeout_s: Optional[float] = None,
        **kwargs: Any,
    ) -> SchemaOptObservation:
        self._state.step_count += 1
        reward = 0.0
        done = False
        status = "ok"
        message = ""
        feedback: Dict[str, Any] = {}

        try:
            if action.operation == "inspect_catalog":
                message = "Catalog summary returned."
                feedback = {
                    "event": "inspect_catalog",
                    "base_tables": [table.name for table in self._task.tables],
                    "derived_object_count": len(self._derived_objects),
                }
            elif action.operation == "inspect_table_stats":
                feedback = self._inspect_table_stats(action.target_id or "")
                message = f"Table stats returned for {action.target_id}."
            elif action.operation == "inspect_cluster":
                feedback = self._inspect_cluster(action.target_id or "")
                message = f"Cluster summary returned for {action.target_id}."
            elif action.operation == "inspect_query":
                feedback = self._inspect_query(action.target_id or "")
                message = f"Query summary returned for {action.target_id}."
            elif action.operation == "inspect_query_plan":
                feedback = self._inspect_query_plan(action.target_id or "")
                message = f"Plan summary returned for {action.target_id}."
            elif action.operation == "inspect_router_status":
                feedback = self._inspect_router_status(action.query_ids)
                message = "Router status returned."
            elif action.operation == "retrieve_queries":
                feedback = self._retrieve_queries(action)
                self._state.retrieval_count += 1
                message = f"Retrieved {len(feedback['matched_queries'])} query summaries."
            elif action.operation == "get_query_context":
                feedback = self._get_query_context(action.query_ids)
                self._state.retrieval_count += 1
                message = f"Returned full context for {len(action.query_ids)} queries."
            elif action.operation == "create_derived_object":
                feedback = self._upsert_derived_object(action, modify=False)
                reward += 0.03
                if self._last_failed_object_name == action.name:
                    reward += 0.04
                    feedback["self_correction_bonus"] = 0.04
                message = f"Created derived object {action.name}."
                self._last_failed_object_name = None
            elif action.operation == "modify_derived_object":
                feedback = self._upsert_derived_object(action, modify=True)
                reward += 0.02
                if self._last_failed_object_name == action.name:
                    reward += 0.04
                    feedback["self_correction_bonus"] = 0.04
                message = f"Modified derived object {action.name}."
                self._last_failed_object_name = None
            elif action.operation == "drop_derived_object":
                feedback = self._drop_derived_object(action.target_id or "")
                reward -= 0.01
                message = f"Dropped derived object {action.target_id}."
            elif action.operation == "list_derived_objects":
                feedback = {"event": "list_derived_objects", "derived_objects": [obj.summary() for obj in self._derived_objects.values()]}
                message = "Derived object list returned."
            elif action.operation == "checkpoint":
                feedback = self._checkpoint(action.name)
                reward += 0.01
                message = "Checkpoint created."
            elif action.operation == "revert_checkpoint":
                feedback = self._revert_checkpoint(action.target_id)
                reward += 0.01
                message = "Checkpoint restored."
            elif action.operation == "benchmark_subset":
                feedback = self._benchmark_action([self._get_visible_query(query_id) for query_id in action.query_ids], None)
                reward += feedback["reward_delta"]
                message = f"Benchmarked {len(action.query_ids)} visible queries."
            elif action.operation == "benchmark_cluster":
                queries = visible_queries_for_cluster(self._task, action.cluster_id or "")
                feedback = self._benchmark_action(queries, action.cluster_id)
                reward += feedback["reward_delta"]
                message = f"Benchmarked cluster {action.cluster_id}."
            elif action.operation == "submit":
                feedback = self._submit_episode()
                reward += feedback["terminal_reward"]
                status = "completed"
                done = True
                message = "Final benchmark submitted."
            else:
                raise ValueError(f"Unsupported action: {action.operation}")
        except Exception as exc:
            status = "error"
            message = f"ERROR: {exc}"
            feedback = {"event": action.operation, "error": str(exc)}
            reward -= 0.12
            self._last_error_signature = str(exc)
            self._state.last_error = str(exc)
            if action.operation in {"create_derived_object", "modify_derived_object"}:
                self._last_failed_object_name = action.name

        self._last_feedback = feedback
        self._state.done = done
        self._state.derived_object_count = len(self._derived_objects)
        self._state.checkpoint_count = len(self._checkpoints)
        self._state.storage_used_multiplier = round(sum(obj.storage_multiplier for obj in self._derived_objects.values()), 4)
        if done:
            self._state.final_score = feedback.get("final_score")

        return self._build_observation(status, message, reward, done, feedback)

    @property
    def state(self) -> State:
        return self._state

    @classmethod
    def latest_report(cls) -> Dict[str, Any]:
        return cls.LAST_GRADER_REPORT

    @classmethod
    def run_baseline(cls) -> Dict[str, Any]:
        env = cls()
        env.reset(task_id="schemaopt_easy_lever")
        hotspot = env._task.clusters[0]
        first_query = visible_queries_for_cluster(env._task, hotspot.cluster_id)[0]
        env.step(
            SchemaOptAction(
                operation="create_derived_object",
                object_kind=hotspot.preferred_object_kind,
                name="baseline_hotspot_object",
                sql_definition=first_query.sql,
                source_objects=list(first_query.tables),
                grain_hint=",".join(first_query.group_by),
                intended_clusters=[hotspot.cluster_id],
                routing_tags=list(first_query.plan_features) + list(first_query.filter_tokens) + list(first_query.columns),
            )
        )
        env.step(SchemaOptAction(operation="benchmark_cluster", cluster_id=hotspot.cluster_id))
        final_obs = env.step(SchemaOptAction(operation="submit"))
        return {
            "score": env.state.final_score,
            "reward": final_obs.reward,
            "done": final_obs.done,
            "message": final_obs.message,
            "benchmark": final_obs.benchmark_context,
            "rubric": final_obs.action_feedback,
        }

    def _build_observation(
        self,
        status: str,
        message: str,
        reward: float,
        done: bool,
        feedback: Dict[str, Any],
    ) -> SchemaOptObservation:
        payload = self._task.reset_payload()
        catalog_summary = payload["catalog_summary"]
        catalog_summary["derived_objects"] = [obj.summary() for obj in self._derived_objects.values()]
        catalog_summary["storage_usage_estimate"] = round(sum(obj.storage_multiplier for obj in self._derived_objects.values()), 4)
        catalog_summary["refresh_cost_estimate"] = round(sum(obj.refresh_cost for obj in self._derived_objects.values()), 4)

        return SchemaOptObservation(
            status=status,
            message=message,
            catalog_summary=catalog_summary,
            workload_summary=payload["workload_summary"],
            retrieval_context=self._retrieval_context,
            benchmark_context=self._benchmark_context,
            action_feedback=feedback,
            reward=round(reward, 6),
            done=done,
            metadata={
                "task": payload["task"],
                "step": self._state.step_count,
                "final_score": self._state.final_score,
            },
        )
    def _inspect_table_stats(self, table_name: str) -> Dict[str, Any]:
        for table in self._task.tables:
            if table.name == table_name:
                return {"event": "inspect_table_stats", "table": table.to_dict()}
        raise ValueError(f"Unknown table: {table_name}")

    def _inspect_cluster(self, cluster_id: str) -> Dict[str, Any]:
        if cluster_id not in self._cluster_lookup:
            raise ValueError(f"Unknown cluster_id: {cluster_id}")
        cluster = self._cluster_lookup[cluster_id]
        return {
            "event": "inspect_cluster",
            "cluster": cluster.to_summary(),
            "example_query_ids": list(cluster.query_ids[:3]),
        }

    def _inspect_query(self, query_id: str) -> Dict[str, Any]:
        query = self._get_visible_query(query_id)
        return {"event": "inspect_query", "query": query.summary()}

    def _inspect_query_plan(self, query_id: str) -> Dict[str, Any]:
        query = self._get_visible_query(query_id)
        decision = self._evaluate_query(query, mark_usage=False)
        return {
            "event": "inspect_query_plan",
            "query_id": query_id,
            "original_plan": self._original_plan_summary(query),
            "current_plan": decision.summary(),
        }

    def _inspect_router_status(self, query_ids: Sequence[str]) -> Dict[str, Any]:
        selected_ids = list(query_ids) if query_ids else list(self._visible_query_lookup.keys())
        routes = []
        for query_id in selected_ids:
            query = self._get_visible_query(query_id)
            routes.append(self._evaluate_query(query, mark_usage=False).summary())
        return {"event": "inspect_router_status", "routes": routes}

    def _retrieve_queries(self, action: SchemaOptAction) -> Dict[str, Any]:
        mode = self._resolve_retrieval_mode(action)
        matches = match_queries(
            self._task,
            mode=mode,
            pattern=action.pattern,
            cluster_id=action.cluster_id,
            tables=action.tables,
            columns=action.columns,
            plan_features=action.plan_features,
            top_k=action.top_k,
        )
        matched_queries = [query.summary() for query in matches]
        matched_clusters = sorted({query.cluster_id for query in matches})
        self._retrieval_context = {
            "last_request": {
                "mode": mode,
                "pattern": action.pattern,
                "cluster_id": action.cluster_id,
                "tables": list(action.tables),
                "columns": list(action.columns),
                "plan_features": list(action.plan_features),
                "top_k": action.top_k,
            },
            "matched_queries": matched_queries,
            "matched_clusters": matched_clusters,
            "retrieval_count": self._state.retrieval_count + 1,
        }
        return {"event": "retrieve_queries", **self._retrieval_context}

    def _get_query_context(self, query_ids: Sequence[str]) -> Dict[str, Any]:
        contexts = []
        for query_id in query_ids:
            query = self._get_visible_query(query_id)
            contexts.append(query.context(similar_query_ids(self._task, query_id)))
        self._retrieval_context = {
            "last_request": {"mode": "get_query_context", "query_ids": list(query_ids)},
            "matched_queries": [context["query_id"] for context in contexts],
            "matched_clusters": sorted({context["cluster_id"] for context in contexts}),
            "retrieval_count": self._state.retrieval_count + 1,
        }
        return {"event": "get_query_context", "query_context": contexts}

    def _upsert_derived_object(self, action: SchemaOptAction, modify: bool) -> Dict[str, Any]:
        name = (action.name or "").strip()
        if not _IDENTIFIER_RE.match(name):
            raise ValueError(f"Invalid derived object name: {name}")
        if not modify and name in self._derived_objects:
            raise ValueError(f"Derived object '{name}' already exists")
        if modify and name not in self._derived_objects:
            raise ValueError(f"Derived object '{name}' does not exist")

        for source in action.source_objects:
            if not self._object_exists(source):
                raise ValueError(f"Unknown source object '{source}'")

        derived_name = f"derived.{name}"
        try:
            self._con.execute(f"DROP TABLE IF EXISTS {derived_name}")
            self._con.execute(f"CREATE TABLE {derived_name} AS ({action.sql_definition})")
        except Exception as exc:
            raise ValueError(f"Failed to materialize {name}: {exc}") from exc

        row_count = int(self._con.execute(f"SELECT COUNT(*) FROM {derived_name}").fetchone()[0])
        cursor = self._con.execute(f"SELECT * FROM {derived_name} LIMIT 0")
        available_columns = [column[0].lower() for column in cursor.description or []]
        grain_dims = [part.strip().lower() for part in (action.grain_hint or "").split(",") if part.strip()]
        routing_tags = [tag.strip().lower() for tag in action.routing_tags if tag.strip()]
        storage_multiplier = self._estimate_storage_multiplier(action.object_kind or "join_matview", action.source_objects, available_columns)
        refresh_cost = self._estimate_refresh_cost(action.object_kind or "join_matview", action.source_objects)

        derived_object = DerivedObject(
            name=name,
            object_kind=action.object_kind or "join_matview",
            sql_definition=action.sql_definition or "",
            source_objects=list(action.source_objects),
            grain_dims=grain_dims,
            intended_clusters=[cluster_id.lower() for cluster_id in action.intended_clusters],
            routing_tags=routing_tags,
            available_columns=available_columns,
            row_count=row_count,
            storage_multiplier=storage_multiplier,
            refresh_cost=refresh_cost,
        )
        self._derived_objects[name] = derived_object
        return {"event": "modify_derived_object" if modify else "create_derived_object", "derived_object": derived_object.summary()}

    def _drop_derived_object(self, name: str) -> Dict[str, Any]:
        if name not in self._derived_objects:
            raise ValueError(f"Unknown derived object: {name}")
        self._con.execute(f"DROP TABLE IF EXISTS derived.{name}")
        removed = self._derived_objects.pop(name)
        return {"event": "drop_derived_object", "removed": removed.summary()}

    def _checkpoint(self, label: Optional[str]) -> Dict[str, Any]:
        checkpoint_label = label or f"checkpoint_{len(self._checkpoints) + 1}"
        snapshot = {
            "label": checkpoint_label,
            "derived_objects": copy.deepcopy(self._derived_objects),
        }
        self._checkpoints.append(snapshot)
        return {"event": "checkpoint", "label": checkpoint_label, "checkpoint_count": len(self._checkpoints)}

    def _revert_checkpoint(self, label: Optional[str]) -> Dict[str, Any]:
        if not self._checkpoints:
            raise ValueError("No checkpoints available")

        if label is None:
            snapshot = self._checkpoints[-1]
        else:
            matching = [entry for entry in self._checkpoints if entry["label"] == label]
            if not matching:
                raise ValueError(f"Unknown checkpoint label: {label}")
            snapshot = matching[-1]

        self._seed_connection()
        restored: Dict[str, DerivedObject] = copy.deepcopy(snapshot["derived_objects"])
        self._derived_objects = {}
        for derived_object in restored.values():
            self._con.execute(f"CREATE TABLE derived.{derived_object.name} AS ({derived_object.sql_definition})")
            self._derived_objects[derived_object.name] = derived_object

        return {
            "event": "revert_checkpoint",
            "label": snapshot["label"],
            "restored_objects": [obj.summary() for obj in self._derived_objects.values()],
        }

    def _benchmark_action(self, queries: Sequence[QuerySpec], cluster_id: Optional[str]) -> Dict[str, Any]:
        summary = self._benchmark_queries(queries, mark_usage=True)
        previous_improvement = self._latest_visible_benchmark.get("gated_improvement", 0.0)
        previous_correctness = self._latest_visible_benchmark.get("correctness_coverage", 1.0)
        reward_delta = max(
            -0.25,
            min(
                0.25,
                (summary["gated_improvement"] - previous_improvement) * 0.60
                + (summary["correctness_coverage"] - previous_correctness) * 0.15
                + summary["usage_bonus"]
                - summary["budget_penalty"],
            ),
        )
        self._latest_visible_benchmark = {
            "gated_improvement": summary["gated_improvement"],
            "correctness_coverage": summary["correctness_coverage"],
        }
        self._benchmark_context = {
            "baseline_weighted_cost": summary["baseline_weighted_cost"],
            "current_weighted_cost": summary["actual_current_weighted_cost"],
            "raw_improvement": summary["raw_improvement"],
            "gated_improvement": summary["gated_improvement"],
            "correctness_coverage": summary["correctness_coverage"],
            "routed_query_count": summary["routed_query_count"],
            "incorrect_query_count": summary["incorrect_query_count"],
            "last_benchmarked_query_ids": [query.query_id for query in queries],
            "last_benchmarked_cluster_id": cluster_id,
            "latest_plan_deltas": summary["plan_deltas"],
        }
        self._state.benchmark_runs += 1
        summary["event"] = "benchmark_cluster" if cluster_id else "benchmark_subset"
        summary["reward_delta"] = round(reward_delta, 6)
        return summary
    def _submit_episode(self) -> Dict[str, Any]:
        self._reset_visible_usage()
        visible_summary = self._benchmark_queries(self._task.visible_queries, mark_usage=True)
        holdout_summary = self._benchmark_queries(self._task.holdout_queries, mark_usage=False)
        migration_score = self._migration_score()
        storage_score = self._storage_efficiency_score()
        correctness_score = round((visible_summary["correctness_coverage"] * 0.6) + (holdout_summary["correctness_coverage"] * 0.4), 6)
        final_score = round(
            0.45 * visible_summary["gated_improvement"]
            + 0.20 * holdout_summary["gated_improvement"]
            + 0.20 * correctness_score
            + 0.10 * migration_score
            + 0.05 * storage_score,
            6,
        )
        terminal_reward = round(final_score, 6)
        self._benchmark_context = {
            "baseline_weighted_cost": visible_summary["baseline_weighted_cost"],
            "current_weighted_cost": visible_summary["actual_current_weighted_cost"],
            "raw_improvement": visible_summary["raw_improvement"],
            "gated_improvement": visible_summary["gated_improvement"],
            "correctness_coverage": visible_summary["correctness_coverage"],
            "routed_query_count": visible_summary["routed_query_count"],
            "incorrect_query_count": visible_summary["incorrect_query_count"],
            "last_benchmarked_query_ids": [query.query_id for query in self._task.visible_queries],
            "last_benchmarked_cluster_id": None,
            "latest_plan_deltas": visible_summary["plan_deltas"],
        }
        self._state.benchmark_runs += 1
        self._state.final_score = final_score
        report = {
            "available": True,
            "task_id": self._task.task_id,
            "episode_id": self._state.episode_id,
            "score": final_score,
            "visible_summary": visible_summary,
            "holdout_summary": holdout_summary,
            "migration_score": migration_score,
            "storage_score": storage_score,
        }
        SchemaOptEnvironment.LAST_GRADER_REPORT = report
        return {
            "event": "submit",
            "final_score": final_score,
            "visible_summary": visible_summary,
            "holdout_summary": holdout_summary,
            "migration_score": migration_score,
            "storage_score": storage_score,
            "terminal_reward": terminal_reward,
        }

    def _benchmark_queries(self, queries: Sequence[QuerySpec], mark_usage: bool) -> Dict[str, Any]:
        baseline_weighted_cost = 0.0
        actual_current_weighted_cost = 0.0
        gated_current_weighted_cost = 0.0
        weight_total = 0.0
        correctness_weight_total = 0.0
        routed_query_count = 0
        incorrect_query_count = 0
        usage_bonus = 0.0
        plan_delta_accumulator = {"depth_delta": 0.0, "operator_delta": 0.0, "runtime_delta_ms": 0.0}
        per_query: List[Dict[str, Any]] = []

        for query in queries:
            decision = self._evaluate_query(query, mark_usage=mark_usage)
            baseline_cost = self._compose_query_cost(
                query,
                query.baseline_runtime_ms,
                query.baseline_plan_depth,
                query.operator_count,
                query.join_complexity,
                query.blocking_complexity,
            )
            current_cost = self._compose_query_cost(
                query,
                decision.runtime_ms,
                decision.plan_depth,
                decision.operator_count,
                decision.join_complexity,
                decision.blocking_complexity,
            )
            weight = query.frequency_weight * query.priority_weight
            baseline_weighted_cost += baseline_cost * weight
            actual_current_weighted_cost += current_cost * weight
            gated_current_weighted_cost += current_cost * weight if decision.correctness_pass else baseline_cost * weight
            weight_total += weight
            correctness_weight_total += weight if decision.correctness_pass else 0.0
            routed_query_count += 1 if decision.routed else 0
            incorrect_query_count += 0 if decision.correctness_pass else 1
            if decision.routed and decision.object_name:
                obj = self._derived_objects[decision.object_name]
                if mark_usage and query.cluster_id not in obj.used_by_clusters and self._cluster_lookup[query.cluster_id].hotspot_rank <= 2:
                    usage_bonus += 0.02
            plan_delta_accumulator["depth_delta"] += query.baseline_plan_depth - decision.plan_depth
            plan_delta_accumulator["operator_delta"] += query.operator_count - decision.operator_count
            plan_delta_accumulator["runtime_delta_ms"] += query.baseline_runtime_ms - decision.runtime_ms
            per_query.append(decision.summary())

        raw_improvement = 0.0 if baseline_weighted_cost == 0 else max(0.0, 1.0 - (actual_current_weighted_cost / baseline_weighted_cost))
        gated_improvement = 0.0 if baseline_weighted_cost == 0 else max(0.0, 1.0 - (gated_current_weighted_cost / baseline_weighted_cost))
        correctness_coverage = 1.0 if weight_total == 0 else correctness_weight_total / weight_total
        budget_penalty = self._budget_penalty()

        return {
            "baseline_weighted_cost": round(baseline_weighted_cost, 6),
            "actual_current_weighted_cost": round(actual_current_weighted_cost, 6),
            "gated_current_weighted_cost": round(gated_current_weighted_cost, 6),
            "raw_improvement": round(raw_improvement, 6),
            "gated_improvement": round(gated_improvement, 6),
            "correctness_coverage": round(correctness_coverage, 6),
            "routed_query_count": routed_query_count,
            "incorrect_query_count": incorrect_query_count,
            "usage_bonus": round(usage_bonus, 6),
            "budget_penalty": round(budget_penalty, 6),
            "plan_deltas": {key: round(value, 4) for key, value in plan_delta_accumulator.items()},
            "per_query": per_query,
            "unused_derived_objects": sorted(name for name, obj in self._derived_objects.items() if not obj.used_by_visible_queries),
        }

    def _evaluate_query(self, query: QuerySpec, mark_usage: bool) -> RouteDecision:
        best_decision = RouteDecision(
            query_id=query.query_id,
            routed=False,
            object_name=None,
            runtime_ms=query.baseline_runtime_ms,
            plan_depth=query.baseline_plan_depth,
            operator_count=query.operator_count,
            join_complexity=query.join_complexity,
            blocking_complexity=query.blocking_complexity,
            correctness_pass=True,
            semantic_score=1.0,
            route_reason="base plan",
        )

        for obj in self._derived_objects.values():
            score = self._semantic_score(query, obj)
            if not score["eligible"]:
                continue
            improvement = self._runtime_improvement(query, obj, score)
            runtime_ms = round(query.baseline_runtime_ms * (1.0 - improvement), 6)
            candidate = RouteDecision(
                query_id=query.query_id,
                routed=True,
                object_name=obj.name,
                runtime_ms=runtime_ms,
                plan_depth=max(1, int(math.ceil(query.baseline_plan_depth * (1.0 - improvement * 0.55)))),
                operator_count=max(3, int(math.ceil(query.operator_count * (1.0 - improvement * 0.60)))),
                join_complexity=max(1, int(math.ceil(query.join_complexity * (1.0 - improvement * 0.70)))),
                blocking_complexity=max(1, int(math.ceil(query.blocking_complexity * (1.0 - improvement * 0.50)))),
                correctness_pass=bool(score["correctness_pass"]),
                semantic_score=round(score["semantic_score"], 6),
                route_reason=score["reason"],
            )
            if candidate.runtime_ms < best_decision.runtime_ms:
                best_decision = candidate

        if mark_usage and best_decision.routed and best_decision.object_name:
            obj = self._derived_objects[best_decision.object_name]
            obj.used_by_visible_queries.add(query.query_id)
            obj.used_by_clusters.add(query.cluster_id)

        return best_decision

    def _semantic_score(self, query: QuerySpec, obj: DerivedObject) -> Dict[str, Any]:
        query_tables = set(table.lower() for table in query.tables)
        source_tables = set(source.lower() for source in obj.source_objects)
        if not query_tables.issubset(source_tables):
            return {"eligible": False, "semantic_score": 0.0, "correctness_pass": False, "reason": "source object mismatch"}

        query_columns = set(query.columns)
        available_columns = set(obj.available_columns)
        column_score = 0.35 if not available_columns else len(query_columns & available_columns) / max(1, len(query_columns))

        query_filters = set(query.filter_tokens)
        if obj.filter_tags:
            filter_score = len(query_filters & obj.filter_tags) / max(1, len(query_filters))
        else:
            filter_score = 1.0

        query_grain = set(query.group_by)
        derived_grain = set(obj.grain_dims)
        if query_grain:
            if obj.object_kind == "agg_matview":
                if query_grain.issubset(derived_grain):
                    grain_score = 1.0
                elif derived_grain and derived_grain.issubset(query_grain):
                    grain_score = 0.35
                else:
                    grain_score = 0.60 if query_grain & derived_grain else 0.25
            else:
                grain_score = 1.0 if query_grain.issubset(available_columns | derived_grain) else 0.55
        else:
            grain_score = 1.0

        cluster_score = 1.0 if not obj.intended_clusters else (1.0 if query.cluster_id.lower() in obj.intended_clusters else 0.55)
        if obj.feature_tags:
            plan_score = len(set(query.plan_features) & obj.feature_tags) / max(1, len(set(query.plan_features)))
        else:
            plan_score = 0.7

        semantic_score = round((0.30 * column_score) + (0.25 * grain_score) + (0.20 * filter_score) + (0.15 * cluster_score) + (0.10 * plan_score), 6)
        eligible = semantic_score >= 0.45
        correctness_pass = bool(column_score >= 0.90 and grain_score >= 0.90 and filter_score >= 0.90)
        return {
            "eligible": eligible,
            "semantic_score": semantic_score,
            "correctness_pass": correctness_pass,
            "reason": f"semantic={semantic_score:.3f}; columns={column_score:.2f}; grain={grain_score:.2f}; filters={filter_score:.2f}",
        }

    def _runtime_improvement(self, query: QuerySpec, obj: DerivedObject, score: Dict[str, Any]) -> float:
        kind_base = {
            "join_matview": 0.28,
            "agg_matview": 0.44,
            "filtered_projection": 0.22,
            "denorm_table": 0.32,
        }[obj.object_kind]
        semantic_score = score["semantic_score"]
        plan_bonus = 0.08 if set(query.plan_features) & obj.feature_tags else 0.0
        cluster_bonus = 0.05 if query.cluster_id.lower() in obj.intended_clusters else (-0.02 if obj.intended_clusters else 0.0)
        improvement = kind_base * (0.55 + semantic_score) + plan_bonus + cluster_bonus
        return max(0.05, min(0.70, improvement))

    def _compose_query_cost(
        self,
        query: QuerySpec,
        runtime_ms: float,
        plan_depth: int,
        operator_count: int,
        join_complexity: int,
        blocking_complexity: int,
    ) -> float:
        runtime_norm = runtime_ms / max(query.baseline_runtime_ms, 1.0)
        depth_norm = plan_depth / max(query.baseline_plan_depth, 1)
        operator_norm = operator_count / max(query.operator_count, 1)
        join_norm = join_complexity / max(query.join_complexity, 1)
        blocking_norm = blocking_complexity / max(query.blocking_complexity, 1)
        return 100.0 * ((0.70 * runtime_norm) + (0.10 * depth_norm) + (0.10 * operator_norm) + (0.05 * join_norm) + (0.05 * blocking_norm))

    def _migration_score(self) -> float:
        max_objects = max(1, self._task.budgets["max_new_derived_objects"])
        object_ratio = len(self._derived_objects) / max_objects
        unused_ratio = 0.0 if not self._derived_objects else len([obj for obj in self._derived_objects.values() if not obj.used_by_visible_queries]) / len(self._derived_objects)
        return round(max(0.0, 1.0 - (0.35 * object_ratio) - (0.65 * unused_ratio)), 6)

    def _storage_efficiency_score(self) -> float:
        storage_limit = max(self._task.budgets["max_storage_multiplier"] - 1.0, 0.01)
        storage_ratio = sum(obj.storage_multiplier for obj in self._derived_objects.values()) / storage_limit
        refresh_ratio = sum(obj.refresh_cost for obj in self._derived_objects.values()) / max(self._task.budgets["max_refresh_cost"], 0.01)
        return round(max(0.0, 1.0 - (0.50 * storage_ratio) - (0.50 * refresh_ratio)), 6)

    def _budget_penalty(self) -> float:
        storage_limit = max(self._task.budgets["max_storage_multiplier"] - 1.0, 0.01)
        used_storage = sum(obj.storage_multiplier for obj in self._derived_objects.values())
        refresh_used = sum(obj.refresh_cost for obj in self._derived_objects.values())
        penalty = 0.0
        if used_storage > storage_limit:
            penalty += min(0.12, (used_storage - storage_limit) * 0.35)
        if refresh_used > self._task.budgets["max_refresh_cost"]:
            penalty += min(0.12, (refresh_used - self._task.budgets["max_refresh_cost"]) * 0.20)
        if len(self._derived_objects) > self._task.budgets["max_new_derived_objects"]:
            penalty += 0.10
        return penalty

    def _estimate_storage_multiplier(self, object_kind: str, source_objects: Sequence[str], available_columns: Sequence[str]) -> float:
        base = {
            "join_matview": 0.11,
            "agg_matview": 0.08,
            "filtered_projection": 0.06,
            "denorm_table": 0.15,
        }[object_kind]
        return round(base + (len(source_objects) - 1) * 0.015 + len(available_columns) * 0.004, 6)

    def _estimate_refresh_cost(self, object_kind: str, source_objects: Sequence[str]) -> float:
        base = {
            "join_matview": 0.28,
            "agg_matview": 0.24,
            "filtered_projection": 0.18,
            "denorm_table": 0.34,
        }[object_kind]
        return round(base + len(source_objects) * 0.05, 6)
    def _resolve_retrieval_mode(self, action: SchemaOptAction) -> str:
        if action.pattern and action.top_k is None and not any([action.cluster_id, action.tables, action.columns, action.plan_features]):
            return "regex"
        if action.pattern and action.top_k is not None:
            return "substring"
        if action.cluster_id:
            return "cluster"
        if action.tables:
            return "table_filter"
        if action.columns:
            return "column_filter"
        if action.plan_features:
            return "plan_filter"
        return "hotspot_rank"

    def _get_visible_query(self, query_id: str) -> QuerySpec:
        if query_id not in self._visible_query_lookup:
            raise ValueError(f"Unknown visible query_id: {query_id}")
        return self._visible_query_lookup[query_id]

    def _original_plan_summary(self, query: QuerySpec) -> Dict[str, Any]:
        return {
            "runtime_ms": query.baseline_runtime_ms,
            "plan_depth": query.baseline_plan_depth,
            "operator_count": query.operator_count,
            "join_complexity": query.join_complexity,
            "blocking_complexity": query.blocking_complexity,
            "plan_features": list(query.plan_features),
        }

    def _object_exists(self, name: str) -> bool:
        base_names = {table.name for table in self._task.tables}
        return name in base_names or name in self._derived_objects or name.startswith("derived.")

    def _reset_visible_usage(self) -> None:
        for obj in self._derived_objects.values():
            obj.used_by_visible_queries.clear()
            obj.used_by_clusters.clear()

    def _seed_connection(self) -> None:
        if self._con is not None:
            self._con.close()
        self._con = duckdb.connect(database=":memory:")
        self._con.execute("CREATE SCHEMA IF NOT EXISTS derived")
        for table in self._task.tables:
            self._con.execute(self._create_table_sql(table))
            rows = self._sample_rows_for_table(table)
            placeholders = ", ".join(["?"] * len(table.columns))
            self._con.executemany(f"INSERT INTO {table.name} VALUES ({placeholders})", rows)

    def _create_table_sql(self, table: Any) -> str:
        column_clause = ", ".join(f"{name} {dtype}" for name, dtype in table.columns)
        return f"CREATE TABLE {table.name} ({column_clause})"

    def _sample_rows_for_table(self, table: Any) -> List[tuple[Any, ...]]:
        if table.name.endswith("fact_activity"):
            return [
                (1, 1, 1, 1, 1, 1, "2024-10-10", 120.0, 42.0, 210, 24, "active", "NA"),
                (2, 1, 2, 2, 1, 2, "2024-10-16", 95.0, 38.0, 180, 16, "won", "EU"),
                (3, 2, 1, 1, 2, 1, "2024-11-03", 180.0, 74.0, 260, 31, "active", "NA"),
                (4, 2, 2, 2, 2, 2, "2024-11-10", 160.0, 68.0, 245, 29, "open", "EU"),
                (5, 3, 3, 3, 3, 3, "2024-12-02", 205.0, 88.0, 310, 36, "active", "APAC"),
                (6, 3, 1, 1, 3, 3, "2024-12-19", 220.0, 90.0, 330, 39, "won", "APAC"),
                (7, 1, 2, 3, 2, 1, "2025-01-06", 135.0, 52.0, 215, 20, "active", "NA"),
                (8, 2, 3, 1, 1, 2, "2025-01-18", 142.0, 61.0, 225, 23, "open", "EU"),
                (9, 3, 1, 2, 3, 3, "2025-02-04", 240.0, 97.0, 360, 42, "active", "APAC"),
                (10, 1, 3, 2, 2, 1, "2025-02-20", 128.0, 48.0, 205, 19, "won", "NA"),
                (11, 2, 2, 3, 1, 2, "2025-03-05", 176.0, 72.0, 250, 28, "active", "EU"),
                (12, 3, 1, 1, 3, 3, "2025-03-21", 255.0, 101.0, 375, 45, "open", "APAC"),
            ]
        if table.name.endswith("dim_account"):
            return [
                (1, "Enterprise", "Pipeline", "US", "Growth"),
                (2, "SMB", "Expansion", "DE", "Premium"),
                (3, "Mid-Market", "Retention", "SG", "Premium"),
            ]
        if table.name.endswith("dim_owner"):
            return [
                (1, "Enterprise", "Senior"),
                (2, "Mid Market", "Mid"),
                (3, "Retention", "Lead"),
            ]
        if table.name.endswith("dim_channel"):
            return [
                (1, "Paid", "Search"),
                (2, "Owned", "Email"),
                (3, "Partner", "Referral"),
            ]
        if table.name.endswith("dim_product"):
            return [
                (1, "Core", "Growth"),
                (2, "Add-on", "Premium"),
                (3, "Platform", "Premium"),
            ]
        if table.name.endswith("dim_geo"):
            return [
                (1, "North America", "Tier1"),
                (2, "Europe", "Tier1"),
                (3, "APAC", "Tier2"),
            ]
        raise ValueError(f"Unknown table kind for {table.name}")





