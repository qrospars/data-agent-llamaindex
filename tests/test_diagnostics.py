from __future__ import annotations

from data_agent_core.analytics.modules._base import ModuleContext
from data_agent_core.analytics.modules.diagnostics import DiagnosticsModule


def test_diagnostics_module_returns_quality_signals(analytics_state: dict[str, str]) -> None:
    context = ModuleContext(db_path=analytics_state["db_path"], state_root=analytics_state["state_root"])
    module = DiagnosticsModule(context=context)
    result = module.run(dataset_name=analytics_state["dataset_name"])

    assert result["findings"]
    assert "missingness" in result["raw_module_outputs"]
