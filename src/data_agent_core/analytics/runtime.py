from __future__ import annotations

from typing import Any

from data_agent_core.connectors.sqlalchemy_connector import SQLAlchemyConnector
from data_agent_core.output.models import QueryExecutionResult
from data_agent_core.sql.executor import SafeSQLExecutor
from data_agent_core.sql.formatter import ensure_limit
from data_agent_core.sql.rewriter import normalize_sql
from data_agent_core.sql.validator import SQLValidator


class AnalyticsSQLRuntime:
    def __init__(
        self,
        db_url: str,
        allowed_tables: list[str],
        default_row_limit: int = 1000,
        max_row_limit: int = 10000,
        query_timeout_seconds: int = 30,
    ) -> None:
        self.default_row_limit = default_row_limit
        self.max_row_limit = max_row_limit
        self.validator = SQLValidator(allowed_tables=allowed_tables, max_row_limit=max_row_limit)
        self.executor = SafeSQLExecutor(
            SQLAlchemyConnector(db_url, query_timeout_seconds=query_timeout_seconds),
            truncate_rows=max_row_limit,
        )

    def execute(self, sql: str, row_limit: int | None = None) -> QueryExecutionResult:
        normalized = normalize_sql(sql)
        with_limit, _, _ = ensure_limit(
            normalized,
            row_limit=row_limit or self.default_row_limit,
            max_row_limit=self.max_row_limit,
        )
        validation = self.validator.validate(with_limit)
        if not validation.passed:
            raise ValueError(f"Unsafe SQL: {validation.errors}")
        return self.executor.execute(with_limit)

    def execute_rows(self, sql: str, row_limit: int | None = None) -> list[dict[str, Any]]:
        result = self.execute(sql, row_limit=row_limit)
        return [
            {column: row[index] for index, column in enumerate(result.columns)}
            for row in result.rows
        ]
