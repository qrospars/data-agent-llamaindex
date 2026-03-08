from __future__ import annotations

from datetime import date, datetime
from typing import Any


def duckdb_relation_to_arrow(rel: Any) -> Any:
    if hasattr(rel, "arrow"):
        return rel.arrow()
    if hasattr(rel, "fetch_arrow_table"):
        return rel.fetch_arrow_table()
    raise TypeError("Relation does not support Arrow conversion")


def _json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def arrow_to_json_preview(table: Any, limit: int = 20) -> dict[str, Any]:
    row_count = int(getattr(table, "num_rows", 0))
    preview = table.slice(0, limit) if row_count > limit and hasattr(table, "slice") else table

    rows: list[dict[str, Any]] = []
    if hasattr(preview, "to_pylist"):
        rows = [
            {key: _json_safe(value) for key, value in row.items()}
            for row in list(preview.to_pylist())
        ]

    schema = getattr(preview, "schema", None)
    columns = list(schema.names) if schema is not None and hasattr(schema, "names") else []
    if not columns and rows:
        columns = list(rows[0].keys())

    return {
        "columns": columns,
        "rows": rows,
        "row_count": row_count,
        "truncated": row_count > limit,
    }


def arrow_schema_summary(table: Any) -> dict[str, Any]:
    fields: list[dict[str, Any]] = []
    schema = getattr(table, "schema", None)
    if schema is not None:
        for field in schema:
            fields.append(
                {
                    "name": field.name,
                    "type": str(field.type),
                    "nullable": bool(field.nullable),
                }
            )

    return {
        "num_rows": int(getattr(table, "num_rows", 0)),
        "num_columns": int(getattr(table, "num_columns", 0)),
        "fields": fields,
    }
