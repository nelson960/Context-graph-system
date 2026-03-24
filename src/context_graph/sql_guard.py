from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import sqlglot
from sqlglot import expressions as exp

from context_graph.catalog_service import CatalogService
from context_graph.exceptions import QueryExecutionError, SqlValidationError
from context_graph.sqlite_utils import connect_readonly_sqlite


ALLOWED_ANONYMOUS_FUNCTIONS = {
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "LOWER",
    "UPPER",
    "ROUND",
    "COALESCE",
    "ABS",
    "SUBSTR",
    "DATE",
    "DATETIME",
    "NULLIF",
}

FORBIDDEN_EXPRESSIONS = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Drop,
    exp.Alter,
    exp.Create,
    exp.Merge,
)


@dataclass(frozen=True)
class SqlValidationResult:
    generated_sql: str
    executed_sql: str
    referenced_views: tuple[str, ...]


@dataclass(frozen=True)
class QueryExecutionResult:
    executed_sql: str
    rows: list[dict[str, Any]]
    row_count: int
    duration_ms: float


class SqlValidator:
    def __init__(self, catalog_service: CatalogService, max_rows: int) -> None:
        self._catalog_service = catalog_service
        self._max_rows = max_rows

    def validate(self, sql: str) -> SqlValidationResult:
        stripped_sql = sql.strip().rstrip(";")
        if not stripped_sql:
            raise SqlValidationError("Generated SQL is empty")
        if "pragma" in stripped_sql.lower():
            raise SqlValidationError("PRAGMA statements are not allowed")
        try:
            expressions = sqlglot.parse(stripped_sql, read="sqlite")
        except sqlglot.errors.ParseError as exc:
            raise SqlValidationError(f"Generated SQL is not syntactically valid: {exc}") from exc
        if len(expressions) != 1:
            raise SqlValidationError("Exactly one SQL statement is allowed per request")
        expression = expressions[0]
        if expression.find(exp.Select) is None:
            raise SqlValidationError("Only SELECT queries are allowed")
        for forbidden_expression in FORBIDDEN_EXPRESSIONS:
            if expression.find(forbidden_expression) is not None:
                raise SqlValidationError("Mutating SQL statements are not allowed")
        cte_names = {
            cte.alias_or_name
            for cte in expression.find_all(exp.CTE)
            if cte.alias_or_name
        }
        alias_to_source: dict[str, str] = {}
        referenced_views: set[str] = set()
        for table in expression.find_all(exp.Table):
            table_name = table.name
            qualifier = table.alias_or_name or table_name
            if table_name in cte_names:
                alias_to_source[qualifier] = table_name
                continue
            if table.db or table.catalog:
                raise SqlValidationError("Database-qualified table names are not allowed")
            if not self._catalog_service.is_allowed_view(table_name):
                raise SqlValidationError(f"View '{table_name}' is not approved for LLM-generated queries")
            referenced_views.add(table_name)
            alias_to_source[qualifier] = table_name
            alias_to_source[table_name] = table_name
        if not referenced_views:
            raise SqlValidationError("Generated SQL must reference at least one approved analytical view")
        if expression.find(exp.Star) is not None:
            raise SqlValidationError("SELECT * is not allowed; enumerate columns explicitly")
        for column in expression.find_all(exp.Column):
            qualifier = column.table
            if not qualifier:
                raise SqlValidationError("All columns must be qualified with a view or alias")
            if qualifier in cte_names:
                continue
            source_view = alias_to_source.get(qualifier)
            if not source_view:
                raise SqlValidationError(f"Unknown column qualifier '{qualifier}'")
            allowed_columns = self._catalog_service.allowed_columns_for_view(source_view)
            if column.name not in allowed_columns:
                raise SqlValidationError(
                    f"Column '{column.name}' is not exposed by approved view '{source_view}'"
                )
        for function in expression.find_all(exp.Anonymous):
            if function.name.upper() not in ALLOWED_ANONYMOUS_FUNCTIONS:
                raise SqlValidationError(f"Function '{function.name}' is not allowed")
        executed_sql = (
            f"SELECT * FROM ({stripped_sql}) AS guarded_query LIMIT {self._max_rows}"
        )
        return SqlValidationResult(
            generated_sql=stripped_sql,
            executed_sql=executed_sql,
            referenced_views=tuple(sorted(referenced_views)),
        )


class SqlExecutor:
    def __init__(self, db_path: Path, timeout_ms: int) -> None:
        self._db_path = db_path
        self._timeout_ms = timeout_ms

    def execute(self, validation_result: SqlValidationResult) -> QueryExecutionResult:
        started = time.perf_counter()
        try:
            with connect_readonly_sqlite(self._db_path) as connection:
                connection.row_factory = sqlite3.Row

                def _abort_if_slow() -> int:
                    elapsed_ms = (time.perf_counter() - started) * 1000.0
                    return 1 if elapsed_ms > self._timeout_ms else 0

                connection.set_progress_handler(_abort_if_slow, 1000)
                rows = connection.execute(validation_result.executed_sql).fetchall()
        except sqlite3.OperationalError as exc:
            if "interrupted" in str(exc).lower():
                raise QueryExecutionError(
                    f"Query execution exceeded the {self._timeout_ms}ms limit"
                ) from exc
            raise QueryExecutionError(f"SQLite execution failed: {exc}") from exc
        duration_ms = (time.perf_counter() - started) * 1000.0
        row_dicts = [self._coerce_row(dict(row)) for row in rows]
        return QueryExecutionResult(
            executed_sql=validation_result.executed_sql,
            rows=row_dicts,
            row_count=len(row_dicts),
            duration_ms=duration_ms,
        )

    def _coerce_row(self, row: dict[str, Any]) -> dict[str, Any]:
        coerced: dict[str, Any] = {}
        for key, value in row.items():
            if isinstance(value, bytes):
                coerced[key] = value.decode("utf-8")
            else:
                coerced[key] = value
        return coerced
