"""Build DuckDB-first Spider task assets for schemaopt_env."""

from __future__ import annotations

import json
import os
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

REPO_ROOT = Path(__file__).resolve().parents[2]
SPIDER_ROOT = REPO_ROOT / "spider_data" / "spider_data"
SPIDER_DB_ROOT = SPIDER_ROOT / "database"
ASSET_ROOT = REPO_ROOT / "schemaopt_env" / "task_assets_spider"
DB_OUTPUT_ROOT = ASSET_ROOT / "databases"

SELECTED_SPIDER_TASKS = [
    ("schemaopt_spider_easy_world_1", "easy", "world_1"),
    ("schemaopt_spider_easy_store_1", "easy", "store_1"),
    ("schemaopt_spider_easy_chinook_1", "easy", "chinook_1"),
    ("schemaopt_spider_medium_college_2", "medium", "college_2"),
    ("schemaopt_spider_medium_sakila_1", "medium", "sakila_1"),
    ("schemaopt_spider_medium_formula_1", "medium", "formula_1"),
    ("schemaopt_spider_hard_baseball_1", "hard", "baseball_1"),
]

DIFFICULTY_CONFIG: Dict[str, Dict[str, Any]] = {
    "easy": {
        "cluster_count": 3,
        "visible_per_cluster": 4,
        "holdout_per_cluster": 2,
        "budgets": {
            "max_new_derived_objects": 3,
            "max_storage_bytes": 12_000_000,
            "max_refresh_runtime_ms": 8000.0,
            "max_steps": 18,
        },
    },
    "medium": {
        "cluster_count": 4,
        "visible_per_cluster": 6,
        "holdout_per_cluster": 3,
        "budgets": {
            "max_new_derived_objects": 4,
            "max_storage_bytes": 24_000_000,
            "max_refresh_runtime_ms": 15000.0,
            "max_steps": 24,
        },
    },
    "hard": {
        "cluster_count": 5,
        "visible_per_cluster": 7,
        "holdout_per_cluster": 4,
        "budgets": {
            "max_new_derived_objects": 5,
            "max_storage_bytes": 40_000_000,
            "max_refresh_runtime_ms": 25000.0,
            "max_steps": 30,
        },
    },
}

ALIAS_TOKEN_RE = re.compile(r'\b[a-zA-Z_][a-zA-Z0-9_]*\.((?:"[^"]+")|(?:[a-zA-Z_][a-zA-Z0-9_]*))')


@dataclass
class ParsedSQL:
    tables: List[str]
    canonical_tables: List[str]
    projection_aliases: List[str]
    group_by: List[str]
    filter_predicates: List[str]
    canonical_filter_predicates: List[str]
    measure_columns: List[str]
    aggregate_functions: List[str]
    normalized_sql: str


@dataclass
class SpiderQueryCandidate:
    db_id: str
    original_sql: str
    sql: str
    parsed: ParsedSQL
    question_count: int
    example_splits: List[str]


@dataclass
class QueryFamily:
    family_key: Tuple[Any, ...]
    db_id: str
    queries: List[SpiderQueryCandidate]


def _normalize_sql(sql: str) -> str:
    return " ".join(sql.strip().rstrip(";").lower().split())


def _canonicalize_table_name(table_name: str) -> str:
    return table_name.replace('"', '').strip().lower()


def _canonicalize_predicate(predicate: str) -> str:
    normalized = " ".join(predicate.strip().lower().split())
    normalized = ALIAS_TOKEN_RE.sub(lambda match: match.group(1), normalized)
    normalized = normalized.replace('"', '')
    normalized = normalized.replace('( ', '(').replace(' )', ')')
    return normalized


def _canonicalize_measure_name(value: str) -> str:
    normalized = value.strip().lower().replace('"', '')
    normalized = re.sub(r'\s+', ' ', normalized)
    if normalized in {"count(*)", "count_star()", "count_star"}:
        return "count_star"
    match = re.fullmatch(r'(count|sum|avg|min|max)\((.+)\)', normalized)
    if match:
        func = match.group(1)
        target = match.group(2).strip()
        if target == "*":
            return "count_star"
        target = target.replace('.', '_')
        target = re.sub(r'[^a-z0-9_]+', '_', target).strip('_')
        return f"{func}_{target}" if target else func
    normalized = normalized.replace('.', '_')
    normalized = re.sub(r'[^a-z0-9_]+', '_', normalized).strip('_')
    return normalized


def _default_result_label(value: str) -> str:
    normalized = value.strip().lower().replace('"', '')
    normalized = re.sub(r'\s+', ' ', normalized)
    if normalized == 'count(*)':
        return 'count_star()'
    return normalized


def _parse_query_tail(sql: str, canonical_outputs: Sequence[str], result_columns: Sequence[str]) -> tuple[list[Dict[str, Any]], int | None]:
    lowered = sql.lower()
    order_idx = lowered.find(' order by ')
    limit_idx = lowered.find(' limit ')
    order_by: List[Dict[str, Any]] = []
    limit: int | None = None
    if limit_idx != -1:
        limit_text = sql[limit_idx + 7:].strip().rstrip(';')
        match = re.match(r'(\d+)', limit_text)
        if match:
            limit = int(match.group(1))
    if order_idx != -1:
        end = limit_idx if limit_idx != -1 and limit_idx > order_idx else len(sql)
        clause = sql[order_idx + 10:end].strip()
        canonical_lookup = {item: result_columns[idx] for idx, item in enumerate(canonical_outputs)}
        for item in _split_sql_list(clause):
            match = re.match(r'(.+?)\s+(asc|desc)\s*$', item, re.IGNORECASE)
            expr = match.group(1).strip() if match else item.strip()
            direction = (match.group(2).lower() if match else 'asc')
            if expr.isdigit():
                idx = int(expr) - 1
                canonical = canonical_outputs[idx] if 0 <= idx < len(canonical_outputs) else expr
                result_label = result_columns[idx] if 0 <= idx < len(result_columns) else expr
            else:
                normalized_expr = _default_result_label(expr)
                canonical = _canonicalize_measure_name(expr)
                result_label = canonical_lookup.get(canonical, normalized_expr)
            order_by.append({
                'expression': expr.lower(),
                'result_label': result_label,
                'canonical_output': canonical,
                'direction': direction,
            })
    return order_by, limit


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _sqlite_decl_to_duckdb(decl_type: str) -> str:
    dtype = (decl_type or "").lower()
    if any(token in dtype for token in ["int"]):
        return "BIGINT"
    if any(token in dtype for token in ["real", "floa", "doub"]):
        return "DOUBLE"
    if any(token in dtype for token in ["dec", "num"]):
        return "DOUBLE"
    if "bool" in dtype:
        return "BOOLEAN"
    if "date" in dtype or "time" in dtype:
        return "VARCHAR"
    if "blob" in dtype:
        return "BLOB"
    return "VARCHAR"


def _coerce_value(value: Any, duck_type: str) -> Any:
    if value in (None, ''):
        return None
    if duck_type == 'BIGINT':
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        try:
            return int(str(value))
        except Exception:
            try:
                return int(float(str(value)))
            except Exception:
                return None
    if duck_type == 'DOUBLE':
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(str(value))
        except Exception:
            return None
    if duck_type == 'BOOLEAN':
        lowered = str(value).strip().lower()
        if lowered in {'1', 'true', 't', 'yes', 'y'}:
            return True
        if lowered in {'0', 'false', 'f', 'no', 'n'}:
            return False
        return None
    return value


def _load_spider_examples() -> Dict[str, List[Dict[str, Any]]]:
    by_db: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for split_name in ["train_spider.json", "train_others.json", "dev.json"]:
        split_path = SPIDER_ROOT / split_name
        for row in json.loads(split_path.read_text(encoding="utf-8")):
            by_db[row["db_id"]].append({"split": split_name.replace('.json', ''), **row})
    return by_db


def _sqlite_table_names(db_id: str) -> set[str]:
    sqlite_path = SPIDER_DB_ROOT / db_id / f"{db_id}.sqlite"
    con = sqlite3.connect(str(sqlite_path))
    try:
        rows = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchall()
        return {row[0].lower() for row in rows}
    finally:
        con.close()


def _prefix_raw_schema(sql: str, table_names: set[str]) -> str:
    def repl(match: re.Match[str]) -> str:
        keyword = match.group(1)
        table = match.group(2)
        if '.' in table or table.strip('"').lower() not in table_names:
            return match.group(0)
        return f"{keyword} raw.{table}"
    return re.sub(r'\b(from|join)\s+([A-Za-z_][A-Za-z0-9_]*)\b', repl, sql, flags=re.IGNORECASE)


def _convert_double_quoted_literals(sql: str) -> str:
    def replacer(match: re.Match[str]) -> str:
        inner = match.group(1).replace("'", "''")
        return f"'{inner}'"
    return re.sub(r'"([^"\\]*(?:\\.[^"\\]*)*)"', replacer, sql)


def _normalize_sqlite_to_duckdb(sql: str, table_names: set[str]) -> str:
    normalized = sql.strip().rstrip(';')
    normalized = normalized.replace('`', '"')
    normalized = _convert_double_quoted_literals(normalized)
    normalized = _prefix_raw_schema(normalized, table_names)
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    return normalized


def _split_sql_list(clause: str) -> List[str]:
    parts: List[str] = []
    current: List[str] = []
    depth = 0
    for char in clause:
        if char == '(':
            depth += 1
        elif char == ')':
            depth = max(0, depth - 1)
        if char == ',' and depth == 0:
            parts.append(''.join(current).strip())
            current = []
        else:
            current.append(char)
    if current:
        parts.append(''.join(current).strip())
    return parts


def _result_columns_from_sql(sql: str) -> List[str]:
    normalized = sql.strip().rstrip(';')
    lowered = normalized.lower()
    select_start = lowered.find('select ')
    from_start = lowered.find(' from ')
    if select_start == -1 or from_start == -1:
        return []
    select_clause = normalized[select_start + 7:from_start]
    result: List[str] = []
    for part in _split_sql_list(select_clause):
        match = re.search(r'as\s+([A-Za-z_][A-Za-z0-9_]*)\s*$', part, re.IGNORECASE)
        result.append(match.group(1).lower() if match else _default_result_label(part))
    return result


def _extract_alias(expression: str) -> str:
    match = re.search(r'\bas\s+([A-Za-z_][A-Za-z0-9_]*)\s*$', expression, re.IGNORECASE)
    return match.group(1) if match else expression.split('.')[-1].strip().strip('"')


def _aggregate_function(expression: str) -> Optional[str]:
    match = re.search(r'\b(count|sum|avg|min|max)\s*\(', expression, re.IGNORECASE)
    return match.group(1).lower() if match else None


def _split_predicates(clause: str) -> List[str]:
    clause = clause.strip()
    if not clause:
        return []
    return [part.strip() for part in re.split(r'\s+AND\s+', clause, flags=re.IGNORECASE) if part.strip()]


def _parse_group_by(clause: str, aliases: Sequence[str]) -> List[str]:
    clause = clause.strip()
    if not clause:
        return []
    items = [item.strip() for item in clause.split(',') if item.strip()]
    result: List[str] = []
    for item in items:
        if item.isdigit():
            idx = int(item) - 1
            if 0 <= idx < len(aliases):
                result.append(aliases[idx])
        else:
            result.append(item.strip('"'))
    return result


def _parse_sql_metadata(sql: str) -> ParsedSQL:
    normalized = sql.strip().rstrip(';')
    lowered = normalized.lower()
    if lowered.count('select') != 1:
        raise ValueError('Only simple single-select queries are supported for Spider schemaopt tasks')
    if any(op in lowered for op in [' union ', ' intersect ', ' except ', ' having ']):
        raise ValueError('Unsupported set-operation or having query for Spider schemaopt task')
    select_start = lowered.find('select ')
    from_start = lowered.find(' from ')
    if select_start == -1 or from_start == -1:
        raise ValueError('Only SELECT queries are supported')
    select_clause = normalized[select_start + 7:from_start]
    after_from = normalized[from_start + 6:]
    where_match = re.search(r'\bwhere\b', after_from, re.IGNORECASE)
    group_match = re.search(r'\bgroup\s+by\b', after_from, re.IGNORECASE)
    order_match = re.search(r'\border\s+by\b', after_from, re.IGNORECASE)
    end_where = len(after_from)
    for match in [group_match, order_match]:
        if match and match.start() < end_where:
            end_where = match.start()
    where_clause = after_from[where_match.end():end_where] if where_match else ''
    group_end = order_match.start() if order_match else len(after_from)
    group_clause = after_from[group_match.end():group_end] if group_match else ''
    parts = _split_sql_list(select_clause)
    aliases = [(_extract_alias(part) or _default_result_label(part)).lower() for part in parts]
    measures = [_canonicalize_measure_name(_extract_alias(part) or _default_result_label(part)) for part in parts if _aggregate_function(part)]
    funcs = [func for part in parts if (func := _aggregate_function(part))]
    filter_predicates = _split_predicates(where_clause)
    tables = [item.strip('"').lower() for item in re.findall(r'(?:from|join)\s+([A-Za-z0-9_\."`]+)', normalized, flags=re.IGNORECASE)]
    return ParsedSQL(
        tables=tables,
        canonical_tables=[_canonicalize_table_name(item) for item in tables],
        projection_aliases=aliases,
        group_by=[item.lower() for item in _parse_group_by(group_clause, aliases)],
        filter_predicates=filter_predicates,
        canonical_filter_predicates=[_canonicalize_predicate(item) for item in filter_predicates],
        measure_columns=measures,
        aggregate_functions=[item.lower() for item in funcs],
        normalized_sql=_normalize_sql(normalized),
    )


def _plan_features(parsed: ParsedSQL, sql: str) -> List[str]:
    features: List[str] = []
    lowered = sql.lower()
    if parsed.aggregate_functions:
        features.append('aggregate')
    if parsed.filter_predicates:
        features.append('filter')
    if len(parsed.canonical_tables) > 1:
        features.append('join')
    if len(parsed.group_by) >= 2:
        features.append('wide_group')
    if 'date_trunc(' in lowered or 'strftime(' in lowered:
        features.append('time_bucket')
    if any(func in parsed.aggregate_functions for func in ['sum', 'avg', 'min', 'max']):
        features.append('numeric_aggregation')
    return sorted(set(features))


def _can_reference_route(base: SpiderQueryCandidate, candidate: SpiderQueryCandidate) -> bool:
    if set(base.parsed.canonical_tables) != set(candidate.parsed.canonical_tables):
        return False
    if set(base.parsed.canonical_filter_predicates) != set(candidate.parsed.canonical_filter_predicates):
        return False
    if not set(candidate.parsed.group_by).issubset(set(base.parsed.group_by)):
        return False
    if not set(candidate.parsed.measure_columns).issubset(set(base.parsed.measure_columns)):
        return False
    if not set(candidate.parsed.aggregate_functions).issubset(set(base.parsed.aggregate_functions)):
        return False
    return True


def _load_candidate_queries(db_id: str, examples_by_db: Dict[str, List[Dict[str, Any]]]) -> List[SpiderQueryCandidate]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    table_names = _sqlite_table_names(db_id)
    for ex in examples_by_db.get(db_id, []):
        normalized_sql = _normalize_sqlite_to_duckdb(ex['query'], table_names)
        grouped[normalized_sql].append(ex)
    candidates: List[SpiderQueryCandidate] = []
    for sql, items in grouped.items():
        try:
            parsed = _parse_sql_metadata(sql)
        except Exception:
            continue
        if not parsed.aggregate_functions:
            continue
        if len(parsed.canonical_tables) == 0:
            continue
        candidates.append(
            SpiderQueryCandidate(
                db_id=db_id,
                original_sql=items[0]['query'],
                sql=sql,
                parsed=parsed,
                question_count=len(items),
                example_splits=sorted({item['split'] for item in items}),
            )
        )
    candidates.sort(key=lambda item: (-item.question_count, -len(item.parsed.group_by), item.sql))
    return candidates


def _family_key(candidate: SpiderQueryCandidate) -> Tuple[Any, ...]:
    return (
        tuple(sorted(candidate.parsed.canonical_tables)),
        tuple(sorted(candidate.parsed.canonical_filter_predicates)),
        tuple(sorted(candidate.parsed.measure_columns)),
        tuple(sorted(candidate.parsed.aggregate_functions)),
    )


def _build_families(db_id: str, candidates: Sequence[SpiderQueryCandidate]) -> List[QueryFamily]:
    grouped: Dict[Tuple[Any, ...], List[SpiderQueryCandidate]] = defaultdict(list)
    for candidate in candidates:
        grouped[_family_key(candidate)].append(candidate)
    families: List[QueryFamily] = []
    for key, items in grouped.items():
        unique_group_bys = {tuple(item.parsed.group_by) for item in items}
        items = sorted(items, key=lambda item: (-len(item.parsed.group_by), -item.question_count, item.sql))
        representative = items[0]
        routable = [item for item in items if _can_reference_route(representative, item)]
        if sum(item.question_count for item in routable) < 2 and len(unique_group_bys) < 2:
            continue
        families.append(QueryFamily(family_key=key, db_id=db_id, queries=routable))
    families.sort(
        key=lambda family: (
            -sum(item.question_count for item in family.queries),
            -len({tuple(item.parsed.group_by) for item in family.queries}),
            family.family_key,
        )
    )
    return families


def _import_duckdb():
    import duckdb  # type: ignore
    return duckdb


def _load_tables_from_existing_duckdb(duckdb_path: Path) -> Optional[List[Dict[str, Any]]]:
    if not duckdb_path.exists():
        return None
    duckdb = _import_duckdb()
    try:
        con = duckdb.connect(str(duckdb_path), read_only=True)
    except Exception:
        return None
    try:
        rows = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='raw' ORDER BY table_name").fetchall()
        if not rows:
            return None
        payload = []
        for (table_name,) in rows:
            columns = con.execute(f'DESCRIBE raw.{_quote_identifier(table_name)}').fetchall()
            row_count = int(con.execute(f'SELECT COUNT(*) FROM raw.{_quote_identifier(table_name)}').fetchone()[0])
            payload.append({
                'name': f'raw.{table_name}',
                'columns': [{'name': str(col[0]), 'type': str(col[1])} for col in columns],
                'row_count': row_count,
            })
        return payload
    finally:
        con.close()


def _convert_sqlite_database(sqlite_path: Path, duckdb_path: Path) -> List[Dict[str, Any]]:
    duckdb = _import_duckdb()
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    if duckdb_path.exists():
        duckdb_path.unlink()
    sqlite_con = sqlite3.connect(str(sqlite_path))
    sqlite_con.row_factory = sqlite3.Row
    duck_con = duckdb.connect(str(duckdb_path))
    duck_con.execute('CREATE SCHEMA raw')
    table_payloads: List[Dict[str, Any]] = []
    try:
        table_rows = sqlite_con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").fetchall()
        for row in table_rows:
            table_name = row['name']
            columns = sqlite_con.execute(f'PRAGMA table_info({_quote_identifier(table_name)})').fetchall()
            if not columns:
                continue
            create_cols = []
            column_payload = []
            column_names = []
            for col in columns:
                col_name = col['name']
                column_names.append(col_name)
                duck_type = _sqlite_decl_to_duckdb(col['type'] or '')
                create_cols.append(f'{_quote_identifier(col_name)} {duck_type}')
                column_payload.append({'name': col_name, 'type': duck_type})
            duck_con.execute(f'CREATE TABLE raw.{_quote_identifier(table_name)} ({", ".join(create_cols)})')
            select_sql = f'SELECT * FROM {_quote_identifier(table_name)}'
            cursor = sqlite_con.execute(select_sql)
            placeholders = ', '.join(['?'] * len(column_names))
            insert_sql = f'INSERT INTO raw.{_quote_identifier(table_name)} VALUES ({placeholders})'
            row_count = 0
            while True:
                batch = cursor.fetchmany(1000)
                if not batch:
                    break
                values = [tuple(_coerce_value(item[name], column_payload[idx]['type']) for idx, name in enumerate(column_names)) for item in batch]
                duck_con.executemany(insert_sql, values)
                row_count += len(values)
            table_payloads.append({'name': f'raw.{table_name}', 'columns': column_payload, 'row_count': row_count})
    finally:
        duck_con.close()
        sqlite_con.close()
    return table_payloads


def _execute_query(con: Any, sql: str) -> Tuple[bool, int, Optional[str]]:
    try:
        row_count = int(con.execute(f'SELECT COUNT(*) FROM ({sql}) AS spider_task_subq').fetchone()[0])
        return True, row_count, None
    except Exception as exc:
        return False, 0, str(exc)


def _validated_candidates(duckdb_path: Path, candidates: Sequence[SpiderQueryCandidate]) -> List[Tuple[SpiderQueryCandidate, int]]:
    duckdb = _import_duckdb()
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        valid: List[Tuple[SpiderQueryCandidate, int]] = []
        for candidate in candidates:
            ok, row_count, _ = _execute_query(con, candidate.sql)
            if ok:
                valid.append((candidate, row_count))
        return valid
    finally:
        con.close()


def _reference_rewrite_feasible(queries: Sequence[Tuple[SpiderQueryCandidate, int]]) -> bool:
    if not queries:
        return False
    representative = max(queries, key=lambda item: (len(item[0].parsed.group_by), item[1], item[0].question_count))[0]
    routed = [item for item, row_count in queries if row_count > 0 and _can_reference_route(representative, item)]
    return len(routed) >= 2 or sum(item.question_count for item in routed) >= 2


def _cluster_payload(task_id: str, cluster_index: int, family: QueryFamily, queries: Sequence[Tuple[SpiderQueryCandidate, int]], representative_query_id: str) -> Dict[str, Any]:
    representative = max(queries, key=lambda item: (len(item[0].parsed.group_by), item[1], item[0].question_count))[0]
    total_frequency = round(sum(item.question_count for item, _ in queries), 4)
    total_weight = round(sum(item.question_count for item, _ in queries), 4)
    top_tables = list(representative.parsed.canonical_tables)
    return {
        'cluster_id': f'{task_id}_cluster_{cluster_index:02d}',
        'label': f'{family.db_id} workload cluster {cluster_index}',
        'business_label': f'Spider {family.db_id} analytics cluster {cluster_index}',
        'query_ids': [],
        'query_count': 0,
        'total_frequency_weight': total_frequency,
        'total_weighted_baseline_cost': total_weight,
        'top_tables': top_tables,
        'common_operator_patterns': _plan_features(representative.parsed, representative.sql),
        'representative_dimensions': list(representative.parsed.group_by),
        'representative_measures': list(representative.parsed.measure_columns),
        'hotspot_rank': cluster_index,
        'preferred_object_kind': 'agg_matview',
        'representative_query_id': representative_query_id,
        'cluster_grain_emphasis': list(representative.parsed.group_by),
        'suggested_exact_derived_shape': {
            'object_kind': 'agg_matview',
            'source_objects': list(representative.parsed.canonical_tables),
            'group_by': list(representative.parsed.group_by),
            'canonical_predicates': list(representative.parsed.canonical_filter_predicates),
            'measure_columns': list(representative.parsed.measure_columns),
            'aggregate_functions': list(representative.parsed.aggregate_functions),
        },
        'reference_rewrite_feasible': True,
    }


def _query_payload(query_id: str, cluster_id: str, candidate: SpiderQueryCandidate, row_count: int, description_suffix: str) -> Dict[str, Any]:
    features = _plan_features(candidate.parsed, candidate.sql)
    freq_weight = round(max(1.0, float(candidate.question_count)), 2)
    columns = list(candidate.parsed.group_by) + list(candidate.parsed.measure_columns)
    result_columns = _result_columns_from_sql(candidate.sql) or [column if idx < len(candidate.parsed.group_by) else _default_result_label(column) for idx, column in enumerate(columns)]
    canonical_output_columns = [column if idx < len(candidate.parsed.group_by) else _canonicalize_measure_name(column) for idx, column in enumerate(result_columns)]
    order_by, limit = _parse_query_tail(candidate.sql, canonical_output_columns, result_columns)
    return {
        'query_id': query_id,
        'sql': candidate.sql,
        'original_sql': candidate.original_sql,
        'normalized_sql': candidate.parsed.normalized_sql,
        'cluster_id': cluster_id,
        'business_tag': f'Spider {candidate.db_id} workload',
        'frequency_weight': freq_weight,
        'priority_weight': 1.0,
        'tables': list(candidate.parsed.canonical_tables),
        'canonical_tables': list(candidate.parsed.canonical_tables),
        'columns': columns,
        'result_columns': result_columns,
        'canonical_output_columns': canonical_output_columns,
        'group_by': list(candidate.parsed.group_by),
        'filter_tokens': [pred.replace(' ', '_') for pred in candidate.parsed.canonical_filter_predicates],
        'filter_predicates': list(candidate.parsed.filter_predicates),
        'measure_columns': list(candidate.parsed.measure_columns),
        'aggregate_functions': list(candidate.parsed.aggregate_functions),
        'order_by': order_by,
        'limit': limit,
        'plan_features': features,
        'description': f'{candidate.db_id} {description_suffix} rows={row_count}',
    }


def _build_task_manifest(task_id: str, difficulty: str, db_id: str, examples_by_db: Dict[str, List[Dict[str, Any]]]) -> Optional[Dict[str, Any]]:
    sqlite_path = SPIDER_DB_ROOT / db_id / f'{db_id}.sqlite'
    if not sqlite_path.exists():
        print(f'Skipping {task_id}: missing sqlite database {sqlite_path}')
        return None
    duckdb_path = DB_OUTPUT_ROOT / f'{task_id}.duckdb'
    tables = _load_tables_from_existing_duckdb(duckdb_path)
    if tables is None:
        tables = _convert_sqlite_database(sqlite_path, duckdb_path)
    else:
        print(f'Reusing existing DuckDB database for {task_id}: {duckdb_path}')
    raw_candidates = _load_candidate_queries(db_id, examples_by_db)
    validated = _validated_candidates(duckdb_path, raw_candidates)
    by_sql = {candidate.sql: (candidate, row_count) for candidate, row_count in validated if row_count > 0}
    families = _build_families(db_id, [candidate for candidate, row_count in validated if row_count > 0])
    feasible_families: List[List[Tuple[SpiderQueryCandidate, int]]] = []
    for family in families:
        family_queries = [(candidate, by_sql[candidate.sql][1]) for candidate in family.queries if candidate.sql in by_sql]
        if _reference_rewrite_feasible(family_queries):
            feasible_families.append(family_queries)
    if not feasible_families:
        print(f'Skipping {task_id}: no feasible query families after DuckDB validation')
        return None
    config = DIFFICULTY_CONFIG[difficulty]
    selected_families = feasible_families[: config['cluster_count']]
    visible_queries: List[Dict[str, Any]] = []
    holdout_queries: List[Dict[str, Any]] = []
    clusters: List[Dict[str, Any]] = []
    for cluster_index, family_queries in enumerate(selected_families, start=1):
        cluster_id = f'{task_id}_cluster_{cluster_index:02d}'
        ordered = sorted(
            family_queries,
            key=lambda item: (-len(item[0].parsed.group_by), -item[0].question_count, -item[1], item[0].sql),
        )
        representative_id = f'{task_id}_vq_c{cluster_index:02d}_01'
        cluster_payload = _cluster_payload(task_id, cluster_index, QueryFamily(tuple(), db_id, [item[0] for item in ordered]), ordered, representative_id)
        visible_target = config['visible_per_cluster']
        holdout_target = config['holdout_per_cluster']
        chosen_visible = [ordered[i % len(ordered)] for i in range(visible_target)]
        remaining = ordered[1:] if len(ordered) > 1 else ordered
        chosen_holdout = [remaining[i % len(remaining)] for i in range(holdout_target)]
        for i, (candidate, row_count) in enumerate(chosen_visible, start=1):
            query_id = f'{task_id}_vq_c{cluster_index:02d}_{i:02d}'
            visible_queries.append(_query_payload(query_id, cluster_id, candidate, row_count, f'visible cluster {cluster_index} query {i}'))
            cluster_payload['query_ids'].append(query_id)
        for i, (candidate, row_count) in enumerate(chosen_holdout, start=1):
            query_id = f'{task_id}_hq_c{cluster_index:02d}_{i:02d}'
            holdout_queries.append(_query_payload(query_id, cluster_id, candidate, row_count, f'holdout cluster {cluster_index} query {i}'))
        cluster_payload['query_count'] = len(cluster_payload['query_ids'])
        cluster_payload['total_frequency_weight'] = round(sum(query['frequency_weight'] for query in visible_queries if query['cluster_id'] == cluster_id), 4)
        cluster_payload['total_weighted_baseline_cost'] = round(sum(query['frequency_weight'] * query['priority_weight'] for query in visible_queries if query['cluster_id'] == cluster_id), 4)
        clusters.append(cluster_payload)
    if not visible_queries or not holdout_queries:
        print(f'Skipping {task_id}: insufficient visible/holdout queries after packaging')
        return None
    clusters.sort(key=lambda item: item['total_weighted_baseline_cost'], reverse=True)
    for rank, cluster in enumerate(clusters, start=1):
        cluster['hotspot_rank'] = rank
    return {
        'task_id': task_id,
        'difficulty': difficulty,
        'domain': f'spider_{db_id}',
        'objective': f'Optimize the Spider {db_id} workload over a DuckDB-converted database by materializing derived objects that reduce measured execution cost while preserving exact query results.',
        'seed_source': str(sqlite_path.relative_to(REPO_ROOT)).replace('\\', '/'),
        'dataset_dir': str((SPIDER_DB_ROOT / db_id).relative_to(REPO_ROOT)).replace('\\', '/'),
        'database_path': str(duckdb_path.relative_to(REPO_ROOT)).replace('\\', '/'),
        'tables': tables,
        'visible_queries': visible_queries,
        'holdout_queries': holdout_queries,
        'clusters': clusters,
        'budgets': DIFFICULTY_CONFIG[difficulty]['budgets'],
        'allowed_object_kinds': ['join_matview', 'agg_matview', 'filtered_projection', 'denorm_table'],
        'engine_capabilities': {
            'engine': 'duckdb',
            'uses_real_duckdb_database': True,
            'uses_real_query_execution': True,
            'uses_explain_plan_metrics': True,
            'rewrite_model': 'explicit_rewritten_sql',
            'source_dataset': 'spider',
            'source_database_engine': 'sqlite',
            'source_db_id': db_id,
            'sql_normalized_for_duckdb': True,
        },
        'validation_metadata': {
            'source_db_id': db_id,
            'validated_under_duckdb': True,
            'raw_candidate_query_count': len(raw_candidates),
            'validated_query_count': len(validated),
            'selected_cluster_count': len(clusters),
        },
    }


def main() -> None:
    examples_by_db = _load_spider_examples()
    ASSET_ROOT.mkdir(parents=True, exist_ok=True)
    DB_OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    requested = set(sys.argv[1:] or [item.strip() for item in os.environ.get('SPIDER_TASK_IDS', '').split(',') if item.strip()])
    for task_id, difficulty, db_id in SELECTED_SPIDER_TASKS:
        if requested and task_id not in requested and db_id not in requested:
            continue
        payload = _build_task_manifest(task_id, difficulty, db_id, examples_by_db)
        if payload is None:
            continue
        output_path = ASSET_ROOT / f'{task_id}.json'
        output_path.write_text(json.dumps(payload, indent=2), encoding='utf-8')
        print(f'Wrote {output_path}')


if __name__ == '__main__':
    main()
