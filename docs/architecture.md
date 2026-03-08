# Architecture

`data-agent-core` now has three layers that can be used independently or together.

## 1) NL->SQL runtime (existing)
Flow:
1. Question enters `QueryAgent`.
2. `SchemaIntrospector` builds prompt context from DB + semantic metadata.
3. `SQLGenerator` creates SQL via LLM.
4. `SQLValidator` + SQL normalization enforce read-only constraints.
5. `SafeSQLExecutor` runs SQL through connector.
6. `AnswerSummarizer` + `ChartSuggester` format response.

## 2) Analytics copilot runtime (new)
Flow:
1. Ingest file(s) into local DuckDB (`.state/analytics.duckdb`).
2. Profile dataset deterministically (`DatasetProfiler`).
3. Build semantic layer (`SemanticInferenceService`) with strict validation.
4. Build analysis plan (`AnalysisPlanner`).
5. Execute deterministic modules (`overview`, `trends`, `segments`, `anomalies`, `diagnostics`).
6. Rank findings (`FindingScorer`).
7. Synthesize executive summary from structured evidence only (`SynthesisService`).

Rule: synthesis consumes `EvidenceBundle`; it does not summarize raw query dumps directly.

## 3) MCP layer (new)
- Tools: ingest/profile/semantic/run-analysis/ask/preview/explain
- Resources: schema/profile/semantic/latest-evidence
- Prompts: executive/diagnostic/deep-dive
- Transports: stdio (default) and streamable HTTP

## Important state
All analytics state is local under `.state/`:
- `analytics.duckdb`
- `datasets.json`
- `semantics/`
- `profiles/`
- `evidence/`

## Key implementation packages
- `src/data_agent_core/storage`
- `src/data_agent_core/analytics`
- `src/data_agent_core/mcp`
- `src/data_agent_core/prompts`
