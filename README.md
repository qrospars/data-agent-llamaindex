# data-agent-core

`data-agent-core` is now a local analytics copilot framework with two explicit layers:
- deterministic evidence generation
- LLM synthesis over structured evidence

The original NL->SQL runtime remains available and unchanged in spirit.

## Core architecture
- NL->SQL/chat runtime (existing): schema introspection, semantic config, read-only SQL validation/execution, summary/chart suggestion.
- Analytics layer (new): ingestion, profiling, semantic inference, deterministic analysis modules, finding scoring, synthesis.
- MCP layer (new): tools/resources/prompts for Claude Desktop or any MCP client.

## What it does
- Connects to SQL databases via SQLAlchemy for query/chat runtime.
- Uses local DuckDB (`.state/analytics.duckdb`) for analytics ingestion and analysis execution.
- Ingests CSV/Parquet datasets and stores registry/semantic/profile/evidence state under `.state/`.
- Builds deterministic dataset profiles and semantic candidates.
- Runs deterministic modules: overview, trends, segments, anomalies, diagnostics.
- Ranks findings with weighted scoring.
- Generates executive summary from ranked findings only (not raw query dumps).
- Exposes MCP tools/resources/prompts for analytics workflows.

## What it does not do (current)
- Write-back/mutating SQL execution paths
- Full dashboard publishing
- Authn/authz and multi-tenant controls
- Root-cause proof (synthesis is constrained and caveated)

## Install
```bash
python -m venv .venv
# Windows
.\\.venv\\Scripts\\activate
# macOS/Linux
# source .venv/bin/activate

pip install -e '.[dev]'
```

## Quickstart (existing NL->SQL)
```bash
data-agent ask "Top customers by revenue" sqlite:///./examples/sqlite_demo/demo.db --llm-provider mock
```

## Quickstart (analytics copilot)
Minimal Python flow using MCP tool handlers directly:

```python
from data_agent_core.mcp.tools_ingest import analytics_ingest_csv
from data_agent_core.mcp.tools_schema import analytics_build_semantic_layer
from data_agent_core.mcp.tools_analysis import analytics_run_analysis_plan
import asyncio

analytics_ingest_csv(
    file_path="tests/fixtures/sales.csv",
    dataset_name="sales",
    replace=True,
)

analytics_build_semantic_layer(dataset_name="sales")

result = asyncio.run(
    analytics_run_analysis_plan(
        dataset_name="sales",
        question="What are the top trends and risks?",
        analysis_mode="executive_summary",
    )
)
print(result["executive_summary"])
```

Expected analysis output includes:
- plan
- top findings
- executive summary
- caveats

## MCP server
Run stdio transport (Claude Desktop-oriented):

```bash
python -c "from data_agent_core.mcp.server import run_stdio; run_stdio()"
```

Run streamable HTTP transport:

```bash
python -c "from data_agent_core.mcp.server import run_streamable_http; run_streamable_http(host='127.0.0.1', port=8765)"
```

Available MCP tools:
- `analytics_ingest_csv`
- `analytics_profile_dataset`
- `analytics_build_semantic_layer`
- `analytics_run_analysis_plan`
- `analytics_ask_data`
- `analytics_preview_table`
- `analytics_explain_metric`

Available resources:
- `dataset://{dataset_name}/schema`
- `dataset://{dataset_name}/profile`
- `dataset://{dataset_name}/semantic_model`
- `dataset://{dataset_name}/latest_evidence`

## State layout
Generated artifacts are persisted under:

```text
.state/
  analytics.duckdb
  datasets.json
  semantics/
  profiles/
  evidence/
```

## Development and tests
```bash
pytest
ruff check .
```

Note: analytics/MCP tests may skip when optional runtime packages are unavailable in the active environment.

## What needs to be done after this implementation
1. Install/update runtime deps in your active environment (`pip install -e '.[dev]'`) so `duckdb`, `pyarrow`, and `mcp` are available.
2. Configure Claude Desktop MCP integration to launch this server (stdio transport) and validate all 7 tools are visible.
3. Add a production-safe semantic validation pass for LLM-generated semantic models (stricter SQL expression parsing and policy tests).
4. Add CLI commands for analytics workflows (`ingest`, `profile`, `run-analysis`) to avoid Python-wrapper usage.
5. Add snapshot-style regression tests for end-to-end executive summaries on stable fixture data.
6. Add docs for deployment profiles (local-only vs service mode) and minimal observability for workflow runs.
