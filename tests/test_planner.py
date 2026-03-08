from __future__ import annotations

import json

from data_agent_core.analytics.models import AnalysisRequest, DatasetProfile, SemanticModel
from data_agent_core.analytics.planner import AnalysisPlanner
from data_agent_core.storage.dataset_registry import DatasetRegistry


def test_planner_enforces_mode_rules(analytics_state: dict[str, str]) -> None:
    registry = DatasetRegistry(state_root=analytics_state["state_root"])
    dataset = registry.get_dataset(analytics_state["dataset_name"])

    profile_path = registry.profiles_dir / "sales.json"
    semantic_path = registry.semantics_dir / "sales.json"
    profile = DatasetProfile.model_validate_json(profile_path.read_text(encoding="utf-8"))
    semantic = SemanticModel.model_validate_json(semantic_path.read_text(encoding="utf-8"))

    planner = AnalysisPlanner(max_tasks=8)
    request = AnalysisRequest(
        dataset_name=dataset.dataset_name,
        question="Give me an executive summary",
        analysis_mode="executive_summary",
    )
    plan = planner.create_plan(request=request, profile=profile, semantic_model=semantic)

    modules = [task.module for task in plan.tasks]
    assert "overview" in modules
    assert "diagnostics" in modules
    assert any(module == "trends" for module in modules)
    assert len(plan.tasks) <= 8
