from __future__ import annotations

from pathlib import Path

import pytest

from data_agent_core.mcp.tools_ingest import analytics_ingest_csv, analytics_preview_table
from data_agent_core.storage.dataset_registry import DatasetRegistry


def test_ingest_csv_registers_dataset(tmp_path: Path) -> None:
    pytest.importorskip("duckdb")

    state_root = tmp_path / ".state"
    db_path = state_root / "analytics.duckdb"
    csv_path = Path(__file__).parent / "fixtures" / "sales.csv"

    result = analytics_ingest_csv(
        file_path=str(csv_path),
        dataset_name="sales",
        replace=True,
        state_root=str(state_root),
        db_path=str(db_path),
    )

    assert result["dataset_name"] == "sales"
    assert result["row_count"] == 12
    assert result["table_name"] == "sales"

    registry = DatasetRegistry(state_root=state_root)
    registered = registry.get_dataset("sales")
    assert registered.table_name == "sales"
    assert registered.row_count == 12


def test_preview_table_returns_rows(analytics_state: dict[str, str]) -> None:
    preview = analytics_preview_table(
        dataset_name=analytics_state["dataset_name"],
        limit=5,
        state_root=analytics_state["state_root"],
        db_path=analytics_state["db_path"],
    )

    assert preview["table_name"] == "sales"
    assert len(preview["schema"]) >= 5
    assert len(preview["preview"]["rows"]) == 5
