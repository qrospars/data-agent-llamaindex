from __future__ import annotations

from pathlib import Path

import pytest

sqlalchemy = pytest.importorskip("sqlalchemy")
create_engine = sqlalchemy.create_engine
text = sqlalchemy.text

from data_agent_core.agents.conversation_agent import ConversationAgent, InMemoryConversationStore
from data_agent_core.agents.query_agent import QueryAgent
from data_agent_core.config.models import AppConfig, ProviderConfig
from data_agent_core.output.conversation_notes import MarkdownConversationLogger


def _build_agent_with_notes(tmp_path: Path) -> ConversationAgent:
    db_path = tmp_path / "notes_demo.db"
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
    notes_dir = tmp_path / "notes"
    logger = MarkdownConversationLogger(notes_dir)
    return ConversationAgent(query_agent, InMemoryConversationStore(), notes_logger=logger)


def test_conversation_notes_written(tmp_path: Path) -> None:
    agent = _build_agent_with_notes(tmp_path)

    response = agent.chat("show one row", session_id="team review")

    assert response.notes_path is not None
    notes_path = Path(response.notes_path)
    assert notes_path.exists()
    content = notes_path.read_text(encoding="utf-8")
    assert "# Conversation Notes: team review" in content
    assert "show one row" in content
    assert "SELECT 1 AS ok" in content

