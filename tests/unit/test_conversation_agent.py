from __future__ import annotations

from pathlib import Path

import pytest

sqlalchemy = pytest.importorskip("sqlalchemy")
create_engine = sqlalchemy.create_engine
text = sqlalchemy.text

from data_agent_core.agents.conversation_agent import ConversationAgent, InMemoryConversationStore
from data_agent_core.agents.query_agent import QueryAgent
from data_agent_core.config.models import AppConfig, ProviderConfig


def _build_conversation_agent(tmp_path: Path) -> ConversationAgent:
    db_path = tmp_path / "conversation.db"
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT)"))
        conn.execute(text("INSERT INTO items(name) VALUES ('axe'), ('bow')"))

    semantic = tmp_path / "semantic.yaml"
    semantic.write_text("project: demo\n")

    query_agent = QueryAgent(
        AppConfig(
            db_url=f"sqlite:///{db_path}",
            semantic_config_path=semantic,
            llm_provider=ProviderConfig(provider="mock"),
        )
    )
    return ConversationAgent(query_agent, InMemoryConversationStore())


def test_chat_query_mode(tmp_path: Path) -> None:
    agent = _build_conversation_agent(tmp_path)

    response = agent.chat("show one row", session_id="s1")

    assert response.mode == "query"
    assert response.row_count >= 1
    assert response.sql is not None
    assert response.sql.lower().startswith("select")


def test_chat_previous_sql_mode(tmp_path: Path) -> None:
    agent = _build_conversation_agent(tmp_path)
    first = agent.chat("show one row", session_id="s1")

    second = agent.chat("what sql did you use previously?", session_id="s1")

    assert second.mode == "meta"
    assert second.sql == first.sql


def test_chat_non_data_mode(tmp_path: Path) -> None:
    agent = _build_conversation_agent(tmp_path)

    response = agent.chat("hello there", session_id="s2")

    assert response.mode == "chat"
    assert "analyze your data" in response.message.lower()


def test_chat_follow_up_uses_context(tmp_path: Path) -> None:
    agent = _build_conversation_agent(tmp_path)
    _ = agent.chat("show one row", session_id="s1")

    response = agent.chat("filter that to one row", session_id="s1")

    assert response.mode == "query"
    assert response.sql is not None
