from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from data_agent_core.analytics.models import ArrowTablePreview, ColumnSchema, IngestResult, TableSchema
from data_agent_core.storage.arrow_utils import arrow_to_json_preview


class DuckDBLoader:
    def __init__(self, db_path: str | Path = ".state/analytics.duckdb") -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def ingest_csv(
        self,
        file_path: str,
        table_name: str,
        replace: bool = False,
        dataset_name: str | None = None,
    ) -> IngestResult:
        self._ensure_duckdb()
        source = Path(file_path)
        if not source.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        table = self._sanitize_table_name(table_name)
        dataset = self._sanitize_table_name(dataset_name or table_name)

        with self._connect() as conn:
            if replace:
                conn.execute(f"DROP VIEW IF EXISTS {self._quote(dataset)}")
                conn.execute(f"DROP TABLE IF EXISTS {self._quote(table)}")
            conn.execute(
                f"CREATE TABLE {self._quote(table)} AS SELECT * FROM read_csv_auto(?, HEADER=TRUE)",
                [str(source)],
            )
            self._create_dataset_alias(conn, dataset, table)
            row_count = int(conn.execute(f"SELECT COUNT(*) FROM {self._quote(table)}").fetchone()[0])

        schema = self.get_schema(table)
        return IngestResult(
            dataset_name=dataset,
            table_name=table,
            row_count=row_count,
            schema=TableSchema(table_name=table, columns=schema),
            created_at=datetime.now(tz=timezone.utc),
        )

    def ingest_parquet(
        self,
        file_path: str,
        table_name: str,
        replace: bool = False,
        dataset_name: str | None = None,
    ) -> IngestResult:
        self._ensure_duckdb()
        source = Path(file_path)
        if not source.exists():
            raise FileNotFoundError(f"Parquet file not found: {file_path}")
        table = self._sanitize_table_name(table_name)
        dataset = self._sanitize_table_name(dataset_name or table_name)

        with self._connect() as conn:
            if replace:
                conn.execute(f"DROP VIEW IF EXISTS {self._quote(dataset)}")
                conn.execute(f"DROP TABLE IF EXISTS {self._quote(table)}")
            conn.execute(
                f"CREATE TABLE {self._quote(table)} AS SELECT * FROM read_parquet(?)",
                [str(source)],
            )
            self._create_dataset_alias(conn, dataset, table)
            row_count = int(conn.execute(f"SELECT COUNT(*) FROM {self._quote(table)}").fetchone()[0])

        schema = self.get_schema(table)
        return IngestResult(
            dataset_name=dataset,
            table_name=table,
            row_count=row_count,
            schema=TableSchema(table_name=table, columns=schema),
            created_at=datetime.now(tz=timezone.utc),
        )

    def list_tables(self) -> list[str]:
        self._ensure_duckdb()
        with self._connect() as conn:
            rows = conn.execute("SHOW TABLES").fetchall()
        return [str(row[0]) for row in rows]

    def get_schema(self, table_name: str) -> list[ColumnSchema]:
        table = self._sanitize_table_name(table_name)
        with self._connect() as conn:
            rows = conn.execute(f"PRAGMA table_info({self._quote_literal(table)})").fetchall()

            columns: list[ColumnSchema] = []
            for _, name, dtype, notnull, _, _ in rows:
                samples = conn.execute(
                    f"SELECT DISTINCT {self._quote(name)} FROM {self._quote(table)} "
                    f"WHERE {self._quote(name)} IS NOT NULL LIMIT 3"
                ).fetchall()
                sample_values = [str(item[0]) for item in samples]
                columns.append(
                    ColumnSchema(
                        name=str(name),
                        duckdb_type=str(dtype),
                        nullable=not bool(notnull),
                        sample_values=sample_values,
                    )
                )
        return columns

    def preview(self, table_name: str, limit: int = 20) -> ArrowTablePreview:
        table = self._sanitize_table_name(table_name)
        with self._connect() as conn:
            rel = conn.sql(f"SELECT * FROM {self._quote(table)} LIMIT {int(limit)}")
            arrow = rel.arrow()
            total = int(conn.execute(f"SELECT COUNT(*) FROM {self._quote(table)}").fetchone()[0])

        payload = arrow_to_json_preview(arrow, limit=limit)
        payload["row_count"] = total
        return ArrowTablePreview.model_validate(payload)

    def db_url(self) -> str:
        return f"duckdb:///{self.db_path.as_posix()}"

    def _create_dataset_alias(self, conn: Any, dataset_name: str, table_name: str) -> None:
        if dataset_name == table_name:
            return
        conn.execute(f"CREATE OR REPLACE VIEW {self._quote(dataset_name)} AS SELECT * FROM {self._quote(table_name)}")

    def _connect(self) -> Any:
        import duckdb

        return duckdb.connect(str(self.db_path))

    def _sanitize_table_name(self, value: str) -> str:
        clean = re.sub(r"[^a-zA-Z0-9_]", "_", value.strip())
        clean = re.sub(r"_+", "_", clean).strip("_").lower()
        if not clean:
            clean = "dataset"
        if clean[0].isdigit():
            clean = f"t_{clean}"
        return clean

    def _quote(self, identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    def _quote_literal(self, text: str) -> str:
        return "'" + text.replace("'", "''") + "'"

    def _ensure_duckdb(self) -> None:
        try:
            import duckdb  # noqa: F401
        except ModuleNotFoundError as exc:
            raise RuntimeError("duckdb is required for analytics storage") from exc
