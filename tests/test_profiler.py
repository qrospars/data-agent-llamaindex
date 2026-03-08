from __future__ import annotations

from data_agent_core.analytics.profiler import DatasetProfiler
from data_agent_core.storage.dataset_registry import DatasetRegistry


def test_profiler_computes_roles_and_ranges(analytics_state: dict[str, str]) -> None:
    registry = DatasetRegistry(state_root=analytics_state["state_root"])
    dataset = registry.get_dataset(analytics_state["dataset_name"])

    profiler = DatasetProfiler(
        db_path=analytics_state["db_path"],
        state_root=analytics_state["state_root"],
    )
    profile = profiler.profile_dataset(dataset_name=dataset.dataset_name, table_name=dataset.table_name)

    assert profile.row_count == 12
    assert profile.date_range["min"] is not None
    assert "order_date" in profile.candidate_time_columns
    assert "revenue" in profile.candidate_measure_columns
    assert "region" in profile.candidate_dimension_columns
    assert any(item.column == "revenue" for item in profile.numeric_profiles)
