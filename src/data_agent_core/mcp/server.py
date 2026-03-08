from __future__ import annotations

from data_agent_core.mcp.prompts import deep_dive_prompt, diagnostic_prompt, executive_summary_prompt
from data_agent_core.mcp.resources import (
    load_dataset_profile_resource,
    load_dataset_schema_resource,
    load_dataset_semantic_resource,
    load_latest_evidence_resource,
)
from data_agent_core.mcp.tools_analysis import analytics_run_analysis_plan
from data_agent_core.mcp.tools_chat import analytics_ask_data
from data_agent_core.mcp.tools_ingest import analytics_ingest_csv, analytics_preview_table
from data_agent_core.mcp.tools_schema import (
    analytics_build_semantic_layer,
    analytics_explain_metric,
    analytics_profile_dataset,
)

try:
    from mcp.server.fastmcp import FastMCP
except ModuleNotFoundError:  # pragma: no cover - optional runtime dependency.
    FastMCP = None  # type: ignore[assignment]


def create_server(
    state_root: str = ".state",
    db_path: str = ".state/analytics.duckdb",
):
    if FastMCP is None:
        raise RuntimeError("mcp package is required to run the MCP server")

    server = FastMCP("data-agent-core-analytics")

    @server.tool(
        name="analytics_ingest_csv",
        description=(
            "Ingest a CSV file into local DuckDB storage. "
            "Returns dataset metadata, schema, and row count."
        ),
    )
    def tool_ingest_csv(file_path: str, dataset_name: str, replace: bool = False) -> dict[str, object]:
        return analytics_ingest_csv(
            file_path=file_path,
            dataset_name=dataset_name,
            replace=replace,
            state_root=state_root,
            db_path=db_path,
        )

    @server.tool(
        name="analytics_profile_dataset",
        description="Profile a registered dataset and return deterministic DatasetProfile stats.",
    )
    def tool_profile_dataset(dataset_name: str) -> dict[str, object]:
        return analytics_profile_dataset(dataset_name=dataset_name, state_root=state_root, db_path=db_path)

    @server.tool(
        name="analytics_build_semantic_layer",
        description=(
            "Build semantic metrics/dimensions from profile+schema. "
            "Returns persisted model path and JSON summary."
        ),
    )
    def tool_build_semantic_layer(
        dataset_name: str,
        business_context: str | None = None,
    ) -> dict[str, object]:
        return analytics_build_semantic_layer(
            dataset_name=dataset_name,
            business_context=business_context,
            state_root=state_root,
            db_path=db_path,
        )

    @server.tool(
        name="analytics_run_analysis_plan",
        description=(
            "Run deterministic analysis modules, rank findings, and synthesize an executive summary "
            "from structured evidence only."
        ),
    )
    async def tool_run_analysis_plan(
        dataset_name: str,
        question: str,
        analysis_mode: str,
    ) -> dict[str, object]:
        return await analytics_run_analysis_plan(
            dataset_name=dataset_name,
            question=question,
            analysis_mode=analysis_mode,
            state_root=state_root,
            db_path=db_path,
        )

    @server.tool(
        name="analytics_ask_data",
        description=(
            "Thin wrapper over NL->SQL chat runtime for dataset-specific questions. "
            "Returns SQL, rows preview, summary, and chart suggestion."
        ),
    )
    def tool_ask_data(dataset_name: str, question: str) -> dict[str, object]:
        return analytics_ask_data(
            dataset_name=dataset_name,
            question=question,
            state_root=state_root,
            db_path=db_path,
        )

    @server.tool(
        name="analytics_preview_table",
        description="Preview table rows and schema for a registered dataset.",
    )
    def tool_preview_table(dataset_name: str, limit: int = 20) -> dict[str, object]:
        return analytics_preview_table(
            dataset_name=dataset_name,
            limit=limit,
            state_root=state_root,
            db_path=db_path,
        )

    @server.tool(
        name="analytics_explain_metric",
        description=(
            "Explain a semantic metric definition, SQL expression, related dimensions, and caveats."
        ),
    )
    def tool_explain_metric(dataset_name: str, metric_name: str) -> dict[str, object]:
        return analytics_explain_metric(
            dataset_name=dataset_name,
            metric_name=metric_name,
            state_root=state_root,
        )

    @server.resource("dataset://{dataset_name}/schema")
    def resource_schema(dataset_name: str) -> dict[str, object]:
        return load_dataset_schema_resource(dataset_name, state_root=state_root, db_path=db_path)

    @server.resource("dataset://{dataset_name}/profile")
    def resource_profile(dataset_name: str) -> dict[str, object]:
        return load_dataset_profile_resource(dataset_name, state_root=state_root)

    @server.resource("dataset://{dataset_name}/semantic_model")
    def resource_semantic(dataset_name: str) -> dict[str, object]:
        return load_dataset_semantic_resource(dataset_name, state_root=state_root)

    @server.resource("dataset://{dataset_name}/latest_evidence")
    def resource_evidence(dataset_name: str) -> dict[str, object]:
        return load_latest_evidence_resource(dataset_name, state_root=state_root)

    @server.prompt(name="executive_summary_prompt")
    def prompt_executive(dataset_name: str, question: str) -> str:
        return executive_summary_prompt(dataset_name, question)

    @server.prompt(name="diagnostic_prompt")
    def prompt_diagnostic(dataset_name: str, question: str) -> str:
        return diagnostic_prompt(dataset_name, question)

    @server.prompt(name="deep_dive_prompt")
    def prompt_deep_dive(dataset_name: str, question: str) -> str:
        return deep_dive_prompt(dataset_name, question)

    return server


def run_stdio(state_root: str = ".state", db_path: str = ".state/analytics.duckdb") -> None:
    server = create_server(state_root=state_root, db_path=db_path)
    server.run(transport="stdio")


def run_streamable_http(
    host: str = "127.0.0.1",
    port: int = 8765,
    state_root: str = ".state",
    db_path: str = ".state/analytics.duckdb",
) -> None:
    server = create_server(state_root=state_root, db_path=db_path)
    server.run(transport="streamable-http", host=host, port=port)
