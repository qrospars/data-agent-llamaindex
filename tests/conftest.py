from __future__ import annotations

from pathlib import Path

import pytest

from data_agent_core.mcp.tools_ingest import analytics_ingest_csv
from data_agent_core.mcp.tools_schema import analytics_build_semantic_layer, analytics_profile_dataset


@pytest.fixture()
def analytics_state(tmp_path: Path) -> dict[str, str]:
    pytest.importorskip("duckdb")

    state_root = tmp_path / ".state"
    db_path = state_root / "analytics.duckdb"
    csv_path = Path(__file__).parent / "fixtures" / "sales.csv"

    analytics_ingest_csv(
        file_path=str(csv_path),
        dataset_name="sales",
        replace=True,
        state_root=str(state_root),
        db_path=str(db_path),
    )
    analytics_profile_dataset(
        dataset_name="sales",
        state_root=str(state_root),
        db_path=str(db_path),
    )
    analytics_build_semantic_layer(
        dataset_name="sales",
        state_root=str(state_root),
        db_path=str(db_path),
    )

    return {
        "state_root": str(state_root),
        "db_path": str(db_path),
        "dataset_name": "sales",
        "csv_path": str(csv_path),
    }
