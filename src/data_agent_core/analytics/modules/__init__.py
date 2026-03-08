from __future__ import annotations

from data_agent_core.analytics.modules.anomalies import AnomaliesModule
from data_agent_core.analytics.modules.diagnostics import DiagnosticsModule
from data_agent_core.analytics.modules.overview import OverviewModule
from data_agent_core.analytics.modules.segments import SegmentsModule
from data_agent_core.analytics.modules.trends import TrendsModule

__all__ = [
    "OverviewModule",
    "TrendsModule",
    "SegmentsModule",
    "AnomaliesModule",
    "DiagnosticsModule",
]
