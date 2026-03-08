from __future__ import annotations

from data_agent_core.analytics.models import SemanticModel
from data_agent_core.analytics.modules._base import ModuleContext
from data_agent_core.analytics.modules.trends import TrendsModule
from data_agent_core.storage.dataset_registry import DatasetRegistry


def test_trends_module_returns_series_and_findings(analytics_state: dict[str, str]) -> None:
    registry = DatasetRegistry(state_root=analytics_state["state_root"])
    semantic = SemanticModel.model_validate_json(
        (registry.semantics_dir / "sales.json").read_text(encoding="utf-8")
    )
    metric_name = semantic.metrics[0].name

    context = ModuleContext(db_path=analytics_state["db_path"], state_root=analytics_state["state_root"])
    module = TrendsModule(context=context)
    result = module.run(dataset_name=analytics_state["dataset_name"], metric=metric_name, grain="month")

    assert result["findings"]
    assert "series" in result["raw_module_outputs"]
    assert isinstance(result["raw_module_outputs"]["series"], list)
