from __future__ import annotations

import asyncio
import importlib.util

import pytest

from data_agent_core.mcp.server import create_server
from data_agent_core.mcp.tools_analysis import analytics_run_analysis_plan
from data_agent_core.mcp.tools_chat import analytics_ask_data
from data_agent_core.mcp.tools_ingest import analytics_preview_table
from data_agent_core.mcp.tools_schema import analytics_explain_metric, analytics_profile_dataset


def test_mcp_tool_handlers_happy_path(analytics_state: dict[str, str]) -> None:
    profile = analytics_profile_dataset(
        dataset_name=analytics_state["dataset_name"],
        state_root=analytics_state["state_root"],
        db_path=analytics_state["db_path"],
    )
    assert profile["dataset_name"] == "sales"

    preview = analytics_preview_table(
        dataset_name=analytics_state["dataset_name"],
        limit=3,
        state_root=analytics_state["state_root"],
        db_path=analytics_state["db_path"],
    )
    assert len(preview["preview"]["rows"]) == 3

    ask = analytics_ask_data(
        dataset_name=analytics_state["dataset_name"],
        question="show one row",
        state_root=analytics_state["state_root"],
        db_path=analytics_state["db_path"],
    )
    assert "sql" in ask

    metric_info = analytics_explain_metric(
        dataset_name=analytics_state["dataset_name"],
        metric_name="total_revenue",
        state_root=analytics_state["state_root"],
    )
    assert metric_info["metric_name"] == "total_revenue"

    analysis = asyncio.run(
        analytics_run_analysis_plan(
            dataset_name=analytics_state["dataset_name"],
            question="Give me top trends and risks",
            analysis_mode="executive_summary",
            state_root=analytics_state["state_root"],
            db_path=analytics_state["db_path"],
        )
    )
    assert "plan" in analysis
    assert "executive_summary" in analysis
    assert analysis["top_findings"]


def test_create_server_requires_mcp_when_missing() -> None:
    if importlib.util.find_spec("mcp") is not None:
        create_server()
        return
    with pytest.raises(RuntimeError):
        create_server()
