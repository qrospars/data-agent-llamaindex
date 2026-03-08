from __future__ import annotations

from data_agent_core.analytics.models import SemanticModel
from data_agent_core.analytics.modules._base import ModuleContext
from data_agent_core.analytics.modules.overview import OverviewModule
from data_agent_core.storage.dataset_registry import DatasetRegistry


def test_overview_module_returns_findings(analytics_state: dict[str, str]) -> None:
    registry = DatasetRegistry(state_root=analytics_state["state_root"])
    semantic = SemanticModel.model_validate_json(
        (registry.semantics_dir / "sales.json").read_text(encoding="utf-8")
    )

    context = ModuleContext(db_path=analytics_state["db_path"], state_root=analytics_state["state_root"])
    module = OverviewModule(context=context)
    result = module.run(dataset_name=analytics_state["dataset_name"], semantic_model=semantic)

    findings = result["findings"]
    assert findings
    assert all(finding.evidence for finding in findings)
    assert "headline_metrics" in result["raw_module_outputs"]
