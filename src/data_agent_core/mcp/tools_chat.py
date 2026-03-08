from __future__ import annotations

from pathlib import Path

from data_agent_core.agents.query_agent import QueryAgent
from data_agent_core.config.models import AppConfig, ProviderConfig
from data_agent_core.storage.dataset_registry import DatasetRegistry
from data_agent_core.storage.duckdb_loader import DuckDBLoader


def analytics_ask_data(
    dataset_name: str,
    question: str,
    state_root: str = ".state",
    db_path: str = ".state/analytics.duckdb",
) -> dict[str, object]:
    registry = DatasetRegistry(state_root=state_root)
    dataset = registry.get_dataset(dataset_name)
    loader = DuckDBLoader(db_path=db_path)

    config = AppConfig(
        db_url=loader.db_url(),
        semantic_config_path=(Path(dataset.semantic_path) if dataset.semantic_path else None),
        llm_provider=ProviderConfig(provider="mock"),
        allowed_tables=[dataset_name, dataset.table_name],
        default_row_limit=100,
        max_row_limit=1000,
    )
    agent = QueryAgent(config)
    result = agent.ask(question)

    rows_preview = [
        {column: row[index] for index, column in enumerate(result.columns)}
        for row in result.rows[:20]
    ]
    return {
        "sql": result.sql,
        "rows_preview": rows_preview,
        "row_count": result.row_count,
        "summary": result.summary,
        "chart_suggestion": result.chart_suggestion,
    }
