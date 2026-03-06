from __future__ import annotations

import time
from typing import Any

from data_agent_core.connectors.base import DatabaseConnector
from data_agent_core.output.models import QueryExecutionResult


class SafeSQLExecutor:
    def __init__(self, connector: DatabaseConnector, truncate_rows: int = 1000) -> None:
        self.connector = connector
        self.truncate_rows = truncate_rows

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> QueryExecutionResult:
        start = time.perf_counter()
        columns, rows = self.connector.execute_readonly(sql, params)
        duration_ms = int((time.perf_counter() - start) * 1000)
        truncated = len(rows) > self.truncate_rows
        payload = rows[: self.truncate_rows]
        return QueryExecutionResult(
            sql=sql,
            columns=columns,
            rows=payload,
            row_count=len(payload),
            duration_ms=duration_ms,
            truncated=truncated,
        )
