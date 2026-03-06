from __future__ import annotations

from typing import Any

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

from data_agent_core.connectors.base import DatabaseConnector


class SQLAlchemyConnector(DatabaseConnector):
    def __init__(self, db_url: str, query_timeout_seconds: int = 30) -> None:
        self.db_url = db_url
        self.query_timeout_seconds = query_timeout_seconds
        self.engine: Engine | None = None

    def connect(self) -> None:
        if self.engine is None:
            self.engine = create_engine(self.db_url, future=True)

    def get_dialect(self) -> str:
        self.connect()
        assert self.engine is not None
        return self.engine.dialect.name

    def list_tables(self) -> list[str]:
        self.connect()
        assert self.engine is not None
        return inspect(self.engine).get_table_names()

    def list_views(self) -> list[str]:
        self.connect()
        assert self.engine is not None
        return inspect(self.engine).get_view_names()

    def get_columns(self, table_name: str) -> list[dict[str, Any]]:
        self.connect()
        assert self.engine is not None
        return inspect(self.engine).get_columns(table_name)

    def execute_readonly(self, sql: str, params: dict[str, Any] | None = None) -> tuple[list[str], list[list[Any]]]:
        self.connect()
        assert self.engine is not None
        with self.engine.connect() as connection:
            result = connection.execute(text(sql), params or {})
            columns = list(result.keys())
            rows = [list(row) for row in result.fetchall()]
        return columns, rows
