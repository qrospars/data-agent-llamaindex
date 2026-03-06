# data-agent-core

`data-agent-core` is a reusable, database-agnostic NL2SQL runtime for local analytics databases.

## What it does
- Connects to local SQL databases via SQLAlchemy.
- Introspects schema and merges semantic config (YAML/JSON).
- Generates SQL from natural language using LlamaIndex-compatible LLM providers.
- Validates and executes read-only SQL safely.
- Returns SQL, rows, metadata, summary, and chart suggestion.
- Supports multi-turn conversational analysis sessions via API and CLI chat.
- Uses semantic metadata (metrics, dimensions, preferred views, business rules) to improve query and answer quality.

## What it does not do (v1)
- Vector search / document RAG
- Dashboard generation
- Multi-user auth
- Write queries / schema migrations

## Quickstart
```bash
python -m venv .venv && source .venv/bin/activate
pip install -e '.[dev]'

data-agent inspect sqlite:///./demo.db
```

## Run with real LLM + real DB
1. Install dependencies and database driver extras.
```bash
pip install -e '.[dev,postgres]'
```
2. Set your Gemini key.
```bash
cp .env.example .env
# edit .env and set GEMINI_API_KEY
```
3. Point to a real database and use `gemini` provider.
```bash
data-agent ask "Top 10 customers by revenue this month" \
  postgresql+psycopg://user:pass@localhost:5432/analytics \
  --semantic-config ./examples/sqlite_demo/semantic.yaml \
  --llm-provider gemini
```

For SQLite, use a file URL like `sqlite:///./my_data.db`.
`data-agent-core` automatically loads `.env` from the current working directory.

## CLI
- `data-agent ask`
- `data-agent chat`
- `data-agent sql`
- `data-agent run-sql`
- `data-agent inspect`
- `data-agent doctor`

## Conversational mode
Run interactive chat mode (single process session memory):
```bash
data-agent chat sqlite:///./examples/sqlite_demo/demo.db \
  --semantic-config ./examples/sqlite_demo/semantic.yaml \
  --llm-provider gemini
```

API mode supports session memory using `/chat` with a `session_id` value.
The conversational agent uses semantic metadata for:
- intent routing and follow-up resolution
- business-style answer summaries
- follow-up question suggestions tied to metrics/dimensions
- automatic Markdown notes under `docs/conversations/<session_id>.md`

## Web UI
Start the API and open the built-in UI:
```bash
uvicorn data_agent_core.api.app:app --reload
```

Then visit `http://127.0.0.1:8000`.
The UI supports:
- conversation chat against `/chat`
- SQL trace display
- auto-rendered charts (`line`/`bar`) from query results
- result table preview and session note path display

## Safety defaults
- Single statement only
- Read-only SELECT/CTE patterns
- Mutating keyword blocking
- Forbidden tables blocking
- Optional allow-list table enforcement
- Row limit enforcement and capping
