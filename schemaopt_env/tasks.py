"""Task catalog for the schema optimization benchmark."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import math
import re
from typing import Any, Dict, List, Sequence


_REPO_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class TableSpec:
    """Static table metadata exposed to the environment."""

    name: str
    columns: tuple[tuple[str, str], ...]
    row_count: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "columns": [{"name": name, "type": dtype} for name, dtype in self.columns],
            "row_count": self.row_count,
        }


@dataclass(frozen=True)
class QuerySpec:
    """A single workload query and its precomputed metadata."""

    query_id: str
    sql: str
    normalized_sql: str
    cluster_id: str
    business_tag: str
    frequency_weight: float
    priority_weight: float
    tables: tuple[str, ...]
    columns: tuple[str, ...]
    group_by: tuple[str, ...]
    filter_tokens: tuple[str, ...]
    plan_features: tuple[str, ...]
    baseline_runtime_ms: float
    baseline_plan_depth: int
    operator_count: int
    join_complexity: int
    blocking_complexity: int
    description: str

    @property
    def weighted_cost(self) -> float:
        return round(self.baseline_runtime_ms * self.frequency_weight * self.priority_weight, 4)

    def summary(self) -> Dict[str, Any]:
        return {
            "query_id": self.query_id,
            "cluster_id": self.cluster_id,
            "business_tag": self.business_tag,
            "frequency_weight": self.frequency_weight,
            "priority_weight": self.priority_weight,
            "weighted_cost": self.weighted_cost,
            "tables": list(self.tables),
            "columns": list(self.columns),
            "plan_features": list(self.plan_features),
            "description": self.description,
        }

    def context(self, similar_ids: Sequence[str]) -> Dict[str, Any]:
        payload = self.summary()
        payload.update(
            {
                "sql": self.sql,
                "group_by": list(self.group_by),
                "filter_tokens": list(self.filter_tokens),
                "baseline_runtime_ms": self.baseline_runtime_ms,
                "baseline_plan_depth": self.baseline_plan_depth,
                "operator_count": self.operator_count,
                "join_complexity": self.join_complexity,
                "blocking_complexity": self.blocking_complexity,
                "similar_query_ids": list(similar_ids),
            }
        )
        return payload


@dataclass(frozen=True)
class ClusterSpec:
    """A workload cluster used for reset summaries and retrieval."""

    cluster_id: str
    label: str
    business_label: str
    query_ids: tuple[str, ...]
    query_count: int
    total_frequency_weight: float
    total_weighted_baseline_cost: float
    top_tables: tuple[str, ...]
    common_operator_patterns: tuple[str, ...]
    representative_dimensions: tuple[str, ...]
    representative_measures: tuple[str, ...]
    hotspot_rank: int
    preferred_object_kind: str

    def to_summary(self) -> Dict[str, Any]:
        return {
            "cluster_id": self.cluster_id,
            "label": self.label,
            "business_label": self.business_label,
            "query_count": self.query_count,
            "total_frequency_weight": self.total_frequency_weight,
            "total_weighted_baseline_cost": self.total_weighted_baseline_cost,
            "top_tables": list(self.top_tables),
            "common_operator_patterns": list(self.common_operator_patterns),
            "representative_dimensions": list(self.representative_dimensions),
            "representative_measures": list(self.representative_measures),
            "hotspot_rank": self.hotspot_rank,
            "preferred_object_kind": self.preferred_object_kind,
        }


@dataclass(frozen=True)
class TaskSpec:
    """A complete schema optimization episode definition."""

    task_id: str
    difficulty: str
    domain: str
    objective: str
    seed_source: str
    tables: tuple[TableSpec, ...]
    visible_queries: tuple[QuerySpec, ...]
    holdout_queries: tuple[QuerySpec, ...]
    clusters: tuple[ClusterSpec, ...]
    budgets: Dict[str, Any]
    allowed_object_kinds: tuple[str, ...]

    @property
    def total_visible_weighted_cost(self) -> float:
        return round(sum(query.weighted_cost for query in self.visible_queries), 4)

    def task_summary(self) -> Dict[str, Any]:
        return {
            "id": self.task_id,
            "difficulty": self.difficulty,
            "domain": self.domain,
            "objective": self.objective,
            "seed_source": self.seed_source,
            "visible_query_count": len(self.visible_queries),
            "holdout_query_count": len(self.holdout_queries),
            "cluster_count": len(self.clusters),
            "budgets": self.budgets,
            "allowed_object_kinds": list(self.allowed_object_kinds),
        }

    def reset_payload(self) -> Dict[str, Any]:
        return {
            "task": {
                "id": self.task_id,
                "objective": self.objective,
                "difficulty": self.difficulty,
                "domain": self.domain,
                "budgets": self.budgets,
                "allowed_object_kinds": list(self.allowed_object_kinds),
                "submission_rules": {
                    "query_sql_visible_at_reset": False,
                    "query_rewrites_allowed": False,
                    "holdout_workload_used_only_on_submit": True,
                },
                "seed_source": self.seed_source,
            },
            "catalog_summary": {
                "schemas": ["base", "derived"],
                "tables": [table.to_dict() for table in self.tables],
                "lineage_edges": _build_lineage_edges(self.tables),
                "derived_objects": [],
                "storage_usage_estimate": 0.0,
                "refresh_cost_estimate": 0.0,
            },
            "workload_summary": {
                "visible_query_count": len(self.visible_queries),
                "holdout_query_count": len(self.holdout_queries),
                "total_weighted_baseline_cost": self.total_visible_weighted_cost,
                "top_hotspot_clusters": [cluster.to_summary() for cluster in self.clusters[: min(3, len(self.clusters))]],
                "all_clusters": [cluster.to_summary() for cluster in self.clusters],
            },
        }


_DIFFICULTY_CONFIG: Dict[str, Dict[str, Any]] = {
    "easy": {
        "visible_count": 12,
        "holdout_count": 6,
        "cluster_count": 3,
        "cluster_weights": [0.56, 0.29, 0.15],
        "base_runtime": 180.0,
        "row_scale": 1,
        "budgets": {
            "max_new_derived_objects": 3,
            "max_storage_multiplier": 1.35,
            "max_refresh_cost": 1.25,
            "max_steps": 18,
        },
    },
    "medium": {
        "visible_count": 30,
        "holdout_count": 12,
        "cluster_count": 4,
        "cluster_weights": [0.38, 0.26, 0.21, 0.15],
        "base_runtime": 310.0,
        "row_scale": 2,
        "budgets": {
            "max_new_derived_objects": 4,
            "max_storage_multiplier": 1.45,
            "max_refresh_cost": 1.40,
            "max_steps": 24,
        },
    },
    "hard": {
        "visible_count": 48,
        "holdout_count": 20,
        "cluster_count": 5,
        "cluster_weights": [0.30, 0.24, 0.20, 0.16, 0.10],
        "base_runtime": 470.0,
        "row_scale": 3,
        "budgets": {
            "max_new_derived_objects": 5,
            "max_storage_multiplier": 1.60,
            "max_refresh_cost": 1.60,
            "max_steps": 30,
        },
    },
}


_SEED_TASKS: list[tuple[str, str, str, int]] = [
    ("schemaopt_easy_lever", "easy", "lever", 1),
    ("schemaopt_easy_pendo", "easy", "pendo", 2),
    ("schemaopt_easy_customer360", "easy", "customer360", 3),
    ("schemaopt_easy_salesforce", "easy", "salesforce", 4),
    ("schemaopt_medium_google_ads", "medium", "google_ads", 5),
    ("schemaopt_medium_ad_reporting", "medium", "ad_reporting", 6),
    ("schemaopt_medium_app_reporting", "medium", "app_reporting", 7),
    ("schemaopt_medium_asana", "medium", "asana", 8),
    ("schemaopt_hard_google_play", "hard", "google_play", 9),
    ("schemaopt_hard_hubspot", "hard", "hubspot", 10),
    ("schemaopt_hard_jira", "hard", "jira", 11),
    ("schemaopt_hard_marketo", "hard", "marketo", 12),
]


_CLUSTER_BLUEPRINTS: list[dict[str, Any]] = [
    {
        "slug": "revenue_rollup",
        "label": "Monthly Revenue Rollup",
        "business_label": "Finance dashboard rollups",
        "tables": ("fact", "account", "owner"),
        "dims": (
            ("metric_month", "date_trunc('month', f.metric_date)"),
            ("segment", "a.segment"),
            ("team", "o.team"),
            ("country", "a.country"),
            ("seniority", "o.seniority"),
            ("lifecycle_stage", "a.lifecycle_stage"),
        ),
        "measures": (
            ("total_revenue", "sum(f.revenue)"),
            ("total_cost", "sum(f.cost)"),
            ("net_revenue", "sum(f.revenue) - sum(f.cost)"),
        ),
        "plan_features": ("join-heavy", "repeated-aggregation", "blocking"),
        "preferred_object_kind": "agg_matview",
    },
    {
        "slug": "channel_efficiency",
        "label": "Channel Efficiency",
        "business_label": "Channel ROI breakdowns",
        "tables": ("fact", "channel"),
        "dims": (
            ("metric_month", "date_trunc('month', f.metric_date)"),
            ("channel_group", "c.channel_group"),
            ("source_name", "c.source_name"),
            ("status", "f.status"),
        ),
        "measures": (
            ("total_sessions", "sum(f.sessions)"),
            ("total_conversions", "sum(f.conversions)"),
            ("total_revenue", "sum(f.revenue)"),
        ),
        "plan_features": ("repeated-aggregation", "blocking", "wide-scan"),
        "preferred_object_kind": "agg_matview",
    },
    {
        "slug": "product_geo",
        "label": "Product Geo Performance",
        "business_label": "Cross-region performance",
        "tables": ("fact", "product", "geo"),
        "dims": (
            ("metric_month", "date_trunc('month', f.metric_date)"),
            ("product_line", "p.product_line"),
            ("price_tier", "p.price_tier"),
            ("region_name", "g.region_name"),
            ("market_tier", "g.market_tier"),
        ),
        "measures": (
            ("total_revenue", "sum(f.revenue)"),
            ("total_cost", "sum(f.cost)"),
            ("total_conversions", "sum(f.conversions)"),
        ),
        "plan_features": ("join-heavy", "deep-plan", "blocking"),
        "preferred_object_kind": "denorm_table",
    },
    {
        "slug": "owner_funnel",
        "label": "Owner Funnel Diagnostics",
        "business_label": "Sales/owner funnel diagnostics",
        "tables": ("fact", "account", "owner"),
        "dims": (
            ("metric_week", "date_trunc('week', f.metric_date)"),
            ("team", "o.team"),
            ("lifecycle_stage", "a.lifecycle_stage"),
            ("plan_tier", "a.plan_tier"),
        ),
        "measures": (
            ("total_sessions", "sum(f.sessions)"),
            ("total_conversions", "sum(f.conversions)"),
            ("total_revenue", "sum(f.revenue)"),
        ),
        "plan_features": ("join-heavy", "deep-plan", "multi-filter"),
        "preferred_object_kind": "join_matview",
    },
    {
        "slug": "high_value_accounts",
        "label": "High Value Account Tracking",
        "business_label": "Selective account monitoring",
        "tables": ("fact", "account", "geo"),
        "dims": (
            ("metric_month", "date_trunc('month', f.metric_date)"),
            ("segment", "a.segment"),
            ("plan_tier", "a.plan_tier"),
            ("region_name", "g.region_name"),
        ),
        "measures": (
            ("total_revenue", "sum(f.revenue)"),
            ("total_cost", "sum(f.cost)"),
        ),
        "plan_features": ("deep-plan", "selective-filter", "blocking"),
        "preferred_object_kind": "filtered_projection",
    },
]


def _normalize_sql(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip().lower()


def _build_lineage_edges(tables: Sequence[TableSpec]) -> list[dict[str, str]]:
    if len(tables) < 2:
        return []

    edges: list[dict[str, str]] = []
    for upstream, downstream in zip(tables, tables[1:]):
        edges.append({"upstream": upstream.name, "downstream": downstream.name})
    return edges


def _seed_source(index: int) -> str:
    return str(_REPO_ROOT / "datasets" / "dacomp-de" / f"dacomp-de-impl-{index:03d}")


def _table_name(domain: str, role: str) -> str:
    return f"{domain}_{role}"


def _base_tables(domain: str, difficulty: str) -> tuple[TableSpec, ...]:
    scale = _DIFFICULTY_CONFIG[difficulty]["row_scale"]
    tables: list[TableSpec] = [
        TableSpec(
            name=_table_name(domain, "fact_activity"),
            columns=(
                ("event_id", "INTEGER"),
                ("account_id", "INTEGER"),
                ("owner_id", "INTEGER"),
                ("channel_id", "INTEGER"),
                ("product_id", "INTEGER"),
                ("geo_id", "INTEGER"),
                ("metric_date", "DATE"),
                ("revenue", "DOUBLE"),
                ("cost", "DOUBLE"),
                ("sessions", "INTEGER"),
                ("conversions", "INTEGER"),
                ("status", "VARCHAR"),
                ("region", "VARCHAR"),
            ),
            row_count=250_000 * scale,
        ),
        TableSpec(
            name=_table_name(domain, "dim_account"),
            columns=(
                ("account_id", "INTEGER"),
                ("segment", "VARCHAR"),
                ("lifecycle_stage", "VARCHAR"),
                ("country", "VARCHAR"),
                ("plan_tier", "VARCHAR"),
            ),
            row_count=14_000 * scale,
        ),
        TableSpec(
            name=_table_name(domain, "dim_owner"),
            columns=(("owner_id", "INTEGER"), ("team", "VARCHAR"), ("seniority", "VARCHAR")),
            row_count=400 * scale,
        ),
        TableSpec(
            name=_table_name(domain, "dim_channel"),
            columns=(("channel_id", "INTEGER"), ("channel_group", "VARCHAR"), ("source_name", "VARCHAR")),
            row_count=90 * scale,
        ),
    ]

    if difficulty in {"medium", "hard"}:
        tables.append(
            TableSpec(
                name=_table_name(domain, "dim_product"),
                columns=(("product_id", "INTEGER"), ("product_line", "VARCHAR"), ("price_tier", "VARCHAR")),
                row_count=180 * scale,
            )
        )
    if difficulty == "hard":
        tables.append(
            TableSpec(
                name=_table_name(domain, "dim_geo"),
                columns=(("geo_id", "INTEGER"), ("region_name", "VARCHAR"), ("market_tier", "VARCHAR")),
                row_count=120 * scale,
            )
        )

    return tuple(tables)


def _roles_to_tables(domain: str, tables: Sequence[TableSpec]) -> Dict[str, str]:
    available = {table.name for table in tables}
    mapping = {
        "fact": _table_name(domain, "fact_activity"),
        "account": _table_name(domain, "dim_account"),
        "owner": _table_name(domain, "dim_owner"),
        "channel": _table_name(domain, "dim_channel"),
        "product": _table_name(domain, "dim_product"),
        "geo": _table_name(domain, "dim_geo"),
    }
    return {role: table for role, table in mapping.items() if table in available}


def _join_clause(table_roles: Sequence[str], role_map: Dict[str, str]) -> str:
    clauses = [f"{role_map['fact']} f"]
    if "account" in table_roles:
        clauses.append(f"join {role_map['account']} a on f.account_id = a.account_id")
    if "owner" in table_roles:
        clauses.append(f"join {role_map['owner']} o on f.owner_id = o.owner_id")
    if "channel" in table_roles:
        clauses.append(f"join {role_map['channel']} c on f.channel_id = c.channel_id")
    if "product" in table_roles and "product" in role_map:
        clauses.append(f"join {role_map['product']} p on f.product_id = p.product_id")
    if "geo" in table_roles and "geo" in role_map:
        clauses.append(f"join {role_map['geo']} g on f.geo_id = g.geo_id")
    return " ".join(clauses)


def _cluster_query_count(total_count: int, cluster_weights: Sequence[float]) -> list[int]:
    raw = [total_count * weight for weight in cluster_weights]
    counts = [max(1, int(math.floor(value))) for value in raw]
    while sum(counts) < total_count:
        idx = max(range(len(raw)), key=lambda item: raw[item] - counts[item])
        counts[idx] += 1
    while sum(counts) > total_count:
        idx = max(range(len(counts)), key=counts.__getitem__)
        if counts[idx] > 1:
            counts[idx] -= 1
        else:
            break
    return counts


def _render_query(
    task_id: str,
    cluster_blueprint: Dict[str, Any],
    difficulty: str,
    index: int,
    holdout: bool,
    role_map: Dict[str, str],
    cluster_rank: int,
) -> QuerySpec:
    dims = list(cluster_blueprint["dims"])
    measures = list(cluster_blueprint["measures"])
    table_roles = cluster_blueprint["tables"]

    selected_dims = [dims[0], dims[1]]
    if len(dims) > 2 and (index % 2 == 0 or difficulty != "easy"):
        selected_dims.append(dims[2])
    if len(dims) > 3 and (difficulty == "hard" or holdout):
        selected_dims.append(dims[3])

    selected_measures = [measures[0], measures[1]]
    if len(measures) > 2 and index % 3 == 0:
        selected_measures = [measures[0], measures[2]]

    date_windows = [
        ("window=rolling_90d", "f.metric_date >= DATE '2024-10-01'"),
        ("window=rolling_180d", "f.metric_date >= DATE '2024-07-01'"),
        ("window=ytd", "f.metric_date >= DATE '2024-01-01'"),
    ]
    status_filters = [
        ("status=active", "f.status = 'active'"),
        ("status=won", "f.status = 'won'"),
        ("status=open", "f.status = 'open'"),
    ]
    narrow_filters = [
        ("segment=enterprise", "a.segment = 'Enterprise'"),
        ("plan_tier=growth", "a.plan_tier = 'Growth'"),
        ("team=mid_market", "o.team = 'Mid Market'"),
        ("region=na", "g.region_name = 'North America'"),
        ("channel=paid", "c.channel_group = 'Paid'"),
        ("product=premium", "p.price_tier = 'Premium'"),
    ]

    window_token, window_sql = date_windows[index % len(date_windows)]
    status_token, status_sql = status_filters[index % len(status_filters)]
    optional_filter = None
    if holdout or difficulty != "easy":
        token, sql_filter = narrow_filters[(index + cluster_rank) % len(narrow_filters)]
        optional_filter = (token, sql_filter)

    filters = [window_sql, status_sql]
    filter_tokens = [window_token, status_token]
    if optional_filter is not None:
        filters.append(optional_filter[1])
        filter_tokens.append(optional_filter[0])

    select_expressions = [f"{expr} AS {alias}" for alias, expr in selected_dims]
    select_expressions.extend(f"{expr} AS {alias}" for alias, expr in selected_measures)

    sql = (
        "SELECT "
        + ", ".join(select_expressions)
        + " FROM "
        + _join_clause(table_roles, role_map)
        + " WHERE "
        + " AND ".join(filters)
        + " GROUP BY "
        + ", ".join(str(position + 1) for position in range(len(selected_dims)))
    )

    query_id_prefix = "hq" if holdout else "vq"
    query_id = f"{task_id}_{query_id_prefix}_{cluster_blueprint['slug']}_{index + 1:02d}"
    all_columns = [alias for alias, _ in selected_dims] + [alias for alias, _ in selected_measures]
    baseline_runtime = _DIFFICULTY_CONFIG[difficulty]["base_runtime"] * (1.0 + cluster_rank * 0.22 + index * 0.015)
    if holdout:
        baseline_runtime *= 1.08

    base_plan_depth = 5 + cluster_rank
    if "deep-plan" in cluster_blueprint["plan_features"]:
        base_plan_depth += 1
    if holdout:
        base_plan_depth += 1

    operator_count = 9 + cluster_rank * 2 + len(table_roles)
    join_complexity = max(1, len(table_roles) - 1)
    blocking_complexity = 2 if "blocking" in cluster_blueprint["plan_features"] else 1
    priority_weight = round(1.0 + (cluster_rank * 0.08) + ((index % 3) * 0.05), 2)
    frequency_weight = round(1.0 + ((cluster_rank + 1) * 0.6) + ((index % 4) * 0.2), 2)

    description = f"{cluster_blueprint['business_label']} query variant {index + 1}"

    return QuerySpec(
        query_id=query_id,
        sql=sql,
        normalized_sql=_normalize_sql(sql),
        cluster_id=f"{task_id}_{cluster_blueprint['slug']}",
        business_tag=cluster_blueprint["business_label"],
        frequency_weight=frequency_weight,
        priority_weight=priority_weight,
        tables=tuple(role_map[role] for role in table_roles if role in role_map),
        columns=tuple(column.lower() for column in all_columns),
        group_by=tuple(alias.lower() for alias, _ in selected_dims),
        filter_tokens=tuple(token.lower() for token in filter_tokens),
        plan_features=tuple(feature.lower() for feature in cluster_blueprint["plan_features"]),
        baseline_runtime_ms=round(baseline_runtime, 3),
        baseline_plan_depth=base_plan_depth,
        operator_count=operator_count,
        join_complexity=join_complexity,
        blocking_complexity=blocking_complexity,
        description=description,
    )


def _build_queries(task_id: str, difficulty: str, role_map: Dict[str, str]) -> tuple[tuple[QuerySpec, ...], tuple[QuerySpec, ...]]:
    config = _DIFFICULTY_CONFIG[difficulty]
    cluster_count = config["cluster_count"]
    selected_clusters = _CLUSTER_BLUEPRINTS[:cluster_count]

    visible_counts = _cluster_query_count(config["visible_count"], config["cluster_weights"][:cluster_count])
    holdout_counts = _cluster_query_count(config["holdout_count"], config["cluster_weights"][:cluster_count])

    visible_queries: list[QuerySpec] = []
    holdout_queries: list[QuerySpec] = []

    for cluster_rank, cluster_blueprint in enumerate(selected_clusters):
        for index in range(visible_counts[cluster_rank]):
            visible_queries.append(
                _render_query(task_id, cluster_blueprint, difficulty, index, False, role_map, cluster_rank)
            )
        for index in range(holdout_counts[cluster_rank]):
            holdout_queries.append(
                _render_query(task_id, cluster_blueprint, difficulty, index + visible_counts[cluster_rank], True, role_map, cluster_rank)
            )

    return tuple(visible_queries), tuple(holdout_queries)


def _build_clusters(task_id: str, visible_queries: Sequence[QuerySpec]) -> tuple[ClusterSpec, ...]:
    cluster_map: Dict[str, list[QuerySpec]] = {}
    blueprint_by_slug = {blueprint["slug"]: blueprint for blueprint in _CLUSTER_BLUEPRINTS}
    for query in visible_queries:
        cluster_map.setdefault(query.cluster_id, []).append(query)

    weighted_order = sorted(
        cluster_map.items(),
        key=lambda item: sum(query.weighted_cost for query in item[1]),
        reverse=True,
    )

    clusters: list[ClusterSpec] = []
    for hotspot_rank, (cluster_id, queries) in enumerate(weighted_order, start=1):
        slug = cluster_id.removeprefix(f"{task_id}_")
        blueprint = blueprint_by_slug[slug]
        tables = sorted({table for query in queries for table in query.tables})
        clusters.append(
            ClusterSpec(
                cluster_id=cluster_id,
                label=blueprint["label"],
                business_label=blueprint["business_label"],
                query_ids=tuple(query.query_id for query in queries),
                query_count=len(queries),
                total_frequency_weight=round(sum(query.frequency_weight for query in queries), 3),
                total_weighted_baseline_cost=round(sum(query.weighted_cost for query in queries), 3),
                top_tables=tuple(tables[:4]),
                common_operator_patterns=tuple(feature.lower() for feature in blueprint["plan_features"]),
                representative_dimensions=tuple(alias.lower() for alias, _ in blueprint["dims"][:3]),
                representative_measures=tuple(alias.lower() for alias, _ in blueprint["measures"]),
                hotspot_rank=hotspot_rank,
                preferred_object_kind=blueprint["preferred_object_kind"],
            )
        )

    return tuple(clusters)


def _objective_for(domain: str, difficulty: str) -> str:
    difficulty_prompt = {
        "easy": "Find one or two derived structures that accelerate the dominant dashboard workload.",
        "medium": "Balance several workload clusters under a moderate storage budget with reusable derived objects.",
        "hard": "Trade off conflicting workload families and hidden holdout robustness under a strict storage budget.",
    }[difficulty]
    return (
        f"Optimize the {domain.replace('_', ' ')} warehouse workload by adding derived logical structures that keep "
        f"query semantics stable while reducing weighted workload cost. {difficulty_prompt}"
    )


def _build_task(task_id: str, difficulty: str, domain: str, seed_index: int) -> TaskSpec:
    tables = _base_tables(domain, difficulty)
    role_map = _roles_to_tables(domain, tables)
    visible_queries, holdout_queries = _build_queries(task_id, difficulty, role_map)
    clusters = _build_clusters(task_id, visible_queries)
    config = _DIFFICULTY_CONFIG[difficulty]
    return TaskSpec(
        task_id=task_id,
        difficulty=difficulty,
        domain=domain,
        objective=_objective_for(domain, difficulty),
        seed_source=_seed_source(seed_index),
        tables=tables,
        visible_queries=visible_queries,
        holdout_queries=holdout_queries,
        clusters=clusters,
        budgets=dict(config["budgets"]),
        allowed_object_kinds=("join_matview", "agg_matview", "filtered_projection", "denorm_table"),
    )


def build_task_catalog() -> Dict[str, TaskSpec]:
    """Build the static benchmark catalog."""

    return {
        task_id: _build_task(task_id, difficulty, domain, seed_index)
        for task_id, difficulty, domain, seed_index in _SEED_TASKS
    }


TASK_CATALOG = build_task_catalog()


def list_task_summaries() -> List[Dict[str, Any]]:
    """Public task list for the /tasks endpoint."""

    return [task.task_summary() for task in TASK_CATALOG.values()]


def get_task(task_id: str) -> TaskSpec:
    """Return a task by id."""

    if task_id not in TASK_CATALOG:
        raise KeyError(f"Unknown task_id: {task_id}")
    return TASK_CATALOG[task_id]


def query_lookup(task: TaskSpec, include_holdout: bool = True) -> Dict[str, QuerySpec]:
    """Build a query lookup table for a task."""

    lookup = {query.query_id: query for query in task.visible_queries}
    if include_holdout:
        lookup.update({query.query_id: query for query in task.holdout_queries})
    return lookup


def similar_query_ids(task: TaskSpec, query_id: str) -> List[str]:
    """Return same-cluster query ids for retrieval context."""

    lookup = query_lookup(task)
    query = lookup[query_id]
    return [
        candidate.query_id
        for candidate in task.visible_queries
        if candidate.cluster_id == query.cluster_id and candidate.query_id != query_id
    ][:5]


def cluster_lookup(task: TaskSpec) -> Dict[str, ClusterSpec]:
    """Return cluster lookup for a task."""

    return {cluster.cluster_id: cluster for cluster in task.clusters}


def visible_queries_for_cluster(task: TaskSpec, cluster_id: str) -> List[QuerySpec]:
    """Return visible queries for a cluster."""

    return [query for query in task.visible_queries if query.cluster_id == cluster_id]


def match_queries(
    task: TaskSpec,
    mode: str,
    pattern: str | None = None,
    cluster_id: str | None = None,
    tables: Sequence[str] | None = None,
    columns: Sequence[str] | None = None,
    plan_features: Sequence[str] | None = None,
    top_k: int | None = None,
) -> List[QuerySpec]:
    """Deterministic retrieval over the visible workload."""

    queries = list(task.visible_queries)
    pattern_lc = (pattern or "").lower()
    tables_lc = {value.lower() for value in (tables or [])}
    columns_lc = {value.lower() for value in (columns or [])}
    plan_features_lc = {value.lower() for value in (plan_features or [])}

    if mode == "regex":
        if not pattern_lc:
            return []
        compiled = re.compile(pattern_lc)
        matches = [query for query in queries if compiled.search(query.normalized_sql)]
    elif mode == "substring":
        if not pattern_lc:
            return []
        matches = [query for query in queries if pattern_lc in query.normalized_sql]
    elif mode == "cluster":
        matches = [query for query in queries if query.cluster_id == cluster_id]
    elif mode == "table_filter":
        matches = [query for query in queries if tables_lc and tables_lc.issubset(set(query.tables))]
    elif mode == "column_filter":
        matches = [query for query in queries if columns_lc and columns_lc.issubset(set(query.columns))]
    elif mode == "plan_filter":
        matches = [query for query in queries if plan_features_lc and plan_features_lc.issubset(set(query.plan_features))]
    elif mode == "hotspot_rank":
        sorted_queries = sorted(queries, key=lambda query: query.weighted_cost, reverse=True)
        matches = sorted_queries[: max(1, top_k or 5)]
    else:
        raise ValueError(f"Unsupported retrieval mode: {mode}")

    matches.sort(key=lambda query: query.query_id)
    return matches[: top_k] if top_k and mode != "hotspot_rank" else matches

