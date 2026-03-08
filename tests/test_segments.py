from __future__ import annotations

from data_agent_core.analytics.models import SemanticModel
from data_agent_core.analytics.modules._base import ModuleContext
from data_agent_core.analytics.modules.segments import SegmentsModule
from data_agent_core.storage.dataset_registry import DatasetRegistry


def test_segments_module_returns_concentration_metrics(analytics_state: dict[str, str]) -> None:
    registry = DatasetRegistry(state_root=analytics_state["state_root"])
    semantic = SemanticModel.model_validate_json(
        (registry.semantics_dir / "sales.json").read_text(encoding="utf-8")
    )
    metric_name = semantic.metrics[0].name
    dimension_name = semantic.dimensions[0].name

    context = ModuleContext(db_path=analytics_state["db_path"], state_root=analytics_state["state_root"])
    module = SegmentsModule(context=context)
    result = module.run(
        dataset_name=analytics_state["dataset_name"],
        metric=metric_name,
        dimension=dimension_name,
    )

    assert result["findings"]
    assert "concentration_index" in result["raw_module_outputs"]
