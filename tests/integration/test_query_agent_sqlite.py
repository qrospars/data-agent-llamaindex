from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("pydantic")
sqlalchemy = pytest.importorskip("sqlalchemy")
create_engine = sqlalchemy.create_engine
text = sqlalchemy.text

from data_agent_core.agents.query_agent import QueryAgent
from data_agent_core.config.models import AppConfig, ProviderConfig


def test_query_agent_end_to_end(tmp_path: Path) -> None:
    db_path = tmp_path / "demo.db"
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT)"))
        conn.execute(text("INSERT INTO items(name) VALUES ('axe'), ('bow')"))

    semantic = tmp_path / "semantic.yaml"
    semantic.write_text("project: demo\n")

    config = AppConfig(
        db_url=f"sqlite:///{db_path}",
        semantic_config_path=semantic,
        llm_provider=ProviderConfig(provider="mock"),
    )
    agent = QueryAgent(config)
    sql_result = agent.generate_sql("show one row")
    assert sql_result.validation_passed
    response = agent.ask("show one row")
    assert response.row_count == 1
