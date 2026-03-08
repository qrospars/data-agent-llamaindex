# Local development

## Install
```bash
pip install -e '.[dev]'
```

## Run tests and lint
```bash
pytest
ruff check .
```

## Analytics/MCP dependencies
The project now depends on:
- `duckdb`
- `duckdb-engine`
- `pyarrow`
- `mcp`

If your environment is stale, reinstall:
```bash
pip install -e '.[dev]' --upgrade
```

## Run MCP server (stdio)
```bash
python -c "from data_agent_core.mcp.server import run_stdio; run_stdio()"
```

## Run MCP server (streamable HTTP)
```bash
python -c "from data_agent_core.mcp.server import run_streamable_http; run_streamable_http(host='127.0.0.1', port=8765)"
```

## Smoke test analytics flow
1. `analytics_ingest_csv`
2. `analytics_build_semantic_layer`
3. `analytics_run_analysis_plan`

## What to do after setup
1. Wire the MCP server into Claude Desktop (stdio transport).
2. Validate all 7 analytics tools are registered.
3. Run fixture-based end-to-end tests before adding new datasets.
4. Review generated semantic metrics and caveats in `.state/semantics/`.
