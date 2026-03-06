# data-agent-core

`data-agent-core` is a reusable, database-agnostic NL2SQL runtime for local analytics databases.

## What it does
- Connects to local SQL databases via SQLAlchemy.
- Introspects schema and merges semantic config (YAML/JSON).
- Generates SQL from natural language using LlamaIndex-compatible LLM providers.
- Validates and executes read-only SQL safely.
- Returns SQL, rows, metadata, summary, and chart suggestion.

## What it does not do (v1)
- Vector search / document RAG
- Dashboard generation
- Multi-user auth
- Write queries / schema migrations

## Quickstart
```bash
python -m venv .venv && source .venv/bin/activate
pip install -e '.[dev]'

data-agent inspect --db-url sqlite:///./demo.db
```

## CLI
- `data-agent ask`
- `data-agent sql`
- `data-agent run-sql`
- `data-agent inspect`
- `data-agent doctor`

## Safety defaults
- Single statement only
- Read-only SELECT/CTE patterns
- Mutating keyword blocking
- Forbidden tables blocking
- Optional allow-list table enforcement
- Row limit enforcement and capping
