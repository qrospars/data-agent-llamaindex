# postgres_demo

Use this demo to connect `data-agent-core` to a local PostgreSQL analytics database for NL->SQL and MCP-driven analytics.

## Setup
1. Start a local Postgres instance and create/load your analytics schema.
2. Install dependencies:
```bash
pip install -e '.[dev,postgres]'
```
3. Run a baseline NL->SQL query:
```bash
data-agent ask "Top 10 customers by revenue" postgresql+psycopg://user:pass@localhost:5432/analytics --llm-provider mock
```

## After setup
1. Mirror a sample export into analytics DuckDB via `analytics_ingest_csv` for deterministic module runs.
2. Build semantic layer with `analytics_build_semantic_layer` and validate generated metrics.
3. Run `analytics_run_analysis_plan` in `executive_summary` mode and confirm evidence-backed findings.
