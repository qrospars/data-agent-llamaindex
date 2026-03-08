from __future__ import annotations

from datetime import datetime, timezone

from data_agent_core.analytics.models import RegisteredDataset
from data_agent_core.storage.dataset_registry import DatasetRegistry
from data_agent_core.storage.duckdb_loader import DuckDBLoader


def analytics_ingest_csv(
    file_path: str,
    dataset_name: str,
    replace: bool = False,
    state_root: str = ".state",
    db_path: str = ".state/analytics.duckdb",
) -> dict[str, object]:
    loader = DuckDBLoader(db_path=db_path)
    registry = DatasetRegistry(state_root=state_root)

    result = loader.ingest_csv(
        file_path=file_path,
        table_name=dataset_name,
        replace=replace,
        dataset_name=dataset_name,
    )

    registry.register_dataset(
        RegisteredDataset(
            dataset_name=result.dataset_name,
            table_name=result.table_name,
            source_path=file_path,
            created_at=datetime.now(tz=timezone.utc),
            row_count=result.row_count,
        )
    )

    return {
        "dataset_name": result.dataset_name,
        "table_name": result.table_name,
        "row_count": result.row_count,
        "schema": result.schema.model_dump(mode="json"),
        "created_at": result.created_at.isoformat(),
    }


def analytics_preview_table(
    dataset_name: str,
    limit: int = 20,
    state_root: str = ".state",
    db_path: str = ".state/analytics.duckdb",
) -> dict[str, object]:
    loader = DuckDBLoader(db_path=db_path)
    registry = DatasetRegistry(state_root=state_root)
    dataset = registry.get_dataset(dataset_name)

    schema = loader.get_schema(dataset.table_name)
    preview = loader.preview(dataset.table_name, limit=limit)
    return {
        "dataset_name": dataset_name,
        "table_name": dataset.table_name,
        "schema": [column.model_dump(mode="json") for column in schema],
        "preview": preview.model_dump(mode="json"),
    }
