from __future__ import annotations

import json
from pathlib import Path

from data_agent_core.analytics.models import ColumnProfile, DatasetProfile, NumericProfile
from data_agent_core.storage.dataset_registry import DatasetRegistry
from data_agent_core.storage.duckdb_loader import DuckDBLoader


class DatasetProfiler:
    def __init__(self, db_path: str | Path = ".state/analytics.duckdb", state_root: str | Path = ".state") -> None:
        self.loader = DuckDBLoader(db_path=db_path)
        self.registry = DatasetRegistry(state_root=state_root)

    def profile_dataset(self, dataset_name: str, table_name: str) -> DatasetProfile:
        schema = self.loader.get_schema(table_name)

        with self.loader._connect() as conn:
            row_count = int(conn.execute(f"SELECT COUNT(*) FROM {self.loader._quote(table_name)}").fetchone()[0])

            column_profiles: list[ColumnProfile] = []
            numeric_profiles: list[NumericProfile] = []
            candidate_time_columns: list[str] = []
            candidate_dimension_columns: list[str] = []
            candidate_measure_columns: list[str] = []
            date_min: str | None = None
            date_max: str | None = None

            for column in schema:
                column_name = column.name
                quoted_col = self.loader._quote(column_name)
                quoted_table = self.loader._quote(table_name)

                null_count = int(
                    conn.execute(
                        f"SELECT SUM(CASE WHEN {quoted_col} IS NULL THEN 1 ELSE 0 END) FROM {quoted_table}"
                    ).fetchone()[0]
                    or 0
                )
                null_rate = (null_count / row_count) if row_count else 0.0

                distinct_count: int | None = None
                if row_count <= 250_000:
                    distinct_count = int(
                        conn.execute(
                            f"SELECT COUNT(DISTINCT {quoted_col}) FROM {quoted_table}"
                        ).fetchone()[0]
                        or 0
                    )

                top_values: list[tuple[str, int]] = []
                if distinct_count is not None and distinct_count <= 100:
                    top_rows = conn.execute(
                        f"SELECT CAST({quoted_col} AS VARCHAR), COUNT(*) AS c "
                        f"FROM {quoted_table} WHERE {quoted_col} IS NOT NULL "
                        f"GROUP BY 1 ORDER BY c DESC LIMIT 5"
                    ).fetchall()
                    top_values = [(str(value), int(count)) for value, count in top_rows]

                column_profiles.append(
                    ColumnProfile(
                        column=column_name,
                        null_rate=round(null_rate, 6),
                        distinct_count=distinct_count,
                        top_values=top_values,
                    )
                )

                role = self._infer_role(column_name, column.duckdb_type, distinct_count, row_count)
                column.inferred_role = role

                if role == "time":
                    candidate_time_columns.append(column_name)
                    min_max = conn.execute(
                        f"SELECT MIN(CAST({quoted_col} AS TIMESTAMP)), MAX(CAST({quoted_col} AS TIMESTAMP)) "
                        f"FROM {quoted_table} WHERE {quoted_col} IS NOT NULL"
                    ).fetchone()
                    if min_max and min_max[0] is not None:
                        date_min = str(min_max[0])
                        date_max = str(min_max[1])
                elif role == "measure":
                    candidate_measure_columns.append(column_name)
                elif role == "dimension":
                    candidate_dimension_columns.append(column_name)

                if self._is_numeric(column.duckdb_type):
                    stats = conn.execute(
                        f"SELECT MIN(CAST({quoted_col} AS DOUBLE)), MAX(CAST({quoted_col} AS DOUBLE)), "
                        f"AVG(CAST({quoted_col} AS DOUBLE)), MEDIAN(CAST({quoted_col} AS DOUBLE)), "
                        f"STDDEV_SAMP(CAST({quoted_col} AS DOUBLE)) "
                        f"FROM {quoted_table} WHERE {quoted_col} IS NOT NULL"
                    ).fetchone()
                    numeric_profiles.append(
                        NumericProfile(
                            column=column_name,
                            min=self._to_float(stats[0]),
                            max=self._to_float(stats[1]),
                            mean=self._to_float(stats[2]),
                            median=self._to_float(stats[3]),
                            stddev=self._to_float(stats[4]),
                        )
                    )

        profile = DatasetProfile(
            dataset_name=dataset_name,
            row_count=row_count,
            date_range={"min": date_min, "max": date_max},
            numeric_profiles=numeric_profiles,
            column_profiles=column_profiles,
            candidate_time_columns=candidate_time_columns,
            candidate_dimension_columns=candidate_dimension_columns,
            candidate_measure_columns=candidate_measure_columns,
        )

        profile_path = self.registry.profiles_dir / f"{dataset_name}.json"
        profile_path.write_text(profile.model_dump_json(indent=2), encoding="utf-8")
        try:
            self.registry.update_profile_path(dataset_name, str(profile_path))
        except KeyError:
            # Dataset may be profiled before registration in tests.
            pass

        return profile

    def _infer_role(
        self,
        column_name: str,
        duckdb_type: str,
        distinct_count: int | None,
        row_count: int,
    ) -> str:
        lower_name = column_name.lower()
        lower_type = duckdb_type.lower()
        uniqueness = (distinct_count / row_count) if row_count and distinct_count is not None else 0.0

        if self._is_time(lower_type) or any(token in lower_name for token in ("date", "time", "_at")):
            return "time"

        if lower_name == "id" or lower_name.endswith("_id"):
            return "id"
        if distinct_count is not None and row_count > 0 and uniqueness > 0.98 and row_count > 20:
            return "id"

        if self._is_numeric(lower_type):
            return "measure"

        if distinct_count is not None and distinct_count <= max(50, int(row_count * 0.2)):
            return "dimension"

        if "char" in lower_type or "text" in lower_type or "string" in lower_type:
            return "dimension"

        return "unknown"

    def _is_numeric(self, duckdb_type: str) -> bool:
        lower = duckdb_type.lower()
        return any(token in lower for token in ("int", "decimal", "numeric", "double", "float", "real"))

    def _is_time(self, duckdb_type: str) -> bool:
        lower = duckdb_type.lower()
        return "date" in lower or "time" in lower

    def _to_float(self, value: object) -> float | None:
        if value is None:
            return None
        return float(value)
