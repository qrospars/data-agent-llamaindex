from __future__ import annotations

from pathlib import Path
from typing import Literal

import typer

from data_agent_core.agents.conversation_agent import ConversationAgent, InMemoryConversationStore
from data_agent_core.config.env_loader import load_env_file
from data_agent_core.agents.query_agent import QueryAgent
from data_agent_core.config.models import AppConfig, ProviderConfig
from data_agent_core.output.conversation_notes import MarkdownConversationLogger

load_env_file()

app = typer.Typer(help="data-agent-core CLI")
CHAT_STORE = InMemoryConversationStore()
NOTES_LOGGER = MarkdownConversationLogger(Path("docs") / "conversations")


def build_agent(
    db_url: str,
    semantic_config: Path | None,
    llm_provider: Literal["gemini", "mock"] = "mock",
    llm_model: str = "gemini-2.5-flash",
    llm_api_key_env: str = "GEMINI_API_KEY",
    llm_temperature: float = 0.0,
) -> QueryAgent:
    config = AppConfig(
        db_url=db_url,
        semantic_config_path=semantic_config,
        llm_provider=ProviderConfig(
            provider=llm_provider,
            model=llm_model,
            api_key_env=llm_api_key_env,
            temperature=llm_temperature,
        ),
    )
    return QueryAgent(config)


def build_conversation_agent(
    db_url: str,
    semantic_config: Path | None,
    llm_provider: Literal["gemini", "mock"] = "mock",
    llm_model: str = "gemini-2.5-flash",
    llm_api_key_env: str = "GEMINI_API_KEY",
    llm_temperature: float = 0.0,
) -> ConversationAgent:
    return ConversationAgent(
        build_agent(
            db_url,
            semantic_config,
            llm_provider=llm_provider,
            llm_model=llm_model,
            llm_api_key_env=llm_api_key_env,
            llm_temperature=llm_temperature,
        ),
        CHAT_STORE,
        notes_logger=NOTES_LOGGER,
    )


@app.command("ask")
def ask(
    question: str,
    db_url: str,
    semantic_config: Path | None = None,
    llm_provider: Literal["gemini", "mock"] = "mock",
    llm_model: str = "gemini-2.5-flash",
    llm_api_key_env: str = "GEMINI_API_KEY",
    llm_temperature: float = 0.0,
) -> None:
    response = build_agent(
        db_url,
        semantic_config,
        llm_provider=llm_provider,
        llm_model=llm_model,
        llm_api_key_env=llm_api_key_env,
        llm_temperature=llm_temperature,
    ).ask(question)
    typer.echo(response.model_dump_json(indent=2))


@app.command("sql")
def sql(
    question: str,
    db_url: str,
    semantic_config: Path | None = None,
    llm_provider: Literal["gemini", "mock"] = "mock",
    llm_model: str = "gemini-2.5-flash",
    llm_api_key_env: str = "GEMINI_API_KEY",
    llm_temperature: float = 0.0,
) -> None:
    result = build_agent(
        db_url,
        semantic_config,
        llm_provider=llm_provider,
        llm_model=llm_model,
        llm_api_key_env=llm_api_key_env,
        llm_temperature=llm_temperature,
    ).generate_sql(question)
    typer.echo(result.model_dump_json(indent=2))


@app.command("run-sql")
def run_sql(
    sql_text: str,
    db_url: str,
    semantic_config: Path | None = None,
    llm_provider: Literal["gemini", "mock"] = "mock",
    llm_model: str = "gemini-2.5-flash",
    llm_api_key_env: str = "GEMINI_API_KEY",
    llm_temperature: float = 0.0,
) -> None:
    result = build_agent(
        db_url,
        semantic_config,
        llm_provider=llm_provider,
        llm_model=llm_model,
        llm_api_key_env=llm_api_key_env,
        llm_temperature=llm_temperature,
    ).run_sql(sql_text)
    typer.echo(result.model_dump_json(indent=2))


@app.command("inspect")
def inspect(
    db_url: str,
    semantic_config: Path | None = None,
    llm_provider: Literal["gemini", "mock"] = "mock",
    llm_model: str = "gemini-2.5-flash",
    llm_api_key_env: str = "GEMINI_API_KEY",
    llm_temperature: float = 0.0,
) -> None:
    agent = build_agent(
        db_url,
        semantic_config,
        llm_provider=llm_provider,
        llm_model=llm_model,
        llm_api_key_env=llm_api_key_env,
        llm_temperature=llm_temperature,
    )
    payload = {
        "tables": agent.connector.list_tables(),
        "views": agent.connector.list_views(),
        "dialect": agent.connector.get_dialect(),
    }
    typer.echo(payload)


@app.command("doctor")
def doctor(
    db_url: str,
    semantic_config: Path | None = None,
    llm_provider: Literal["gemini", "mock"] = "mock",
    llm_model: str = "gemini-2.5-flash",
    llm_api_key_env: str = "GEMINI_API_KEY",
    llm_temperature: float = 0.0,
) -> None:
    agent = build_agent(
        db_url,
        semantic_config,
        llm_provider=llm_provider,
        llm_model=llm_model,
        llm_api_key_env=llm_api_key_env,
        llm_temperature=llm_temperature,
    )
    checks = {
        "db_connectivity": bool(agent.connector.get_dialect()),
        "semantic_loaded": bool(agent.semantic.project),
        "llm_provider": agent.config.llm_provider.provider,
    }
    typer.echo(checks)


@app.command("chat")
def chat(
    db_url: str,
    session_id: str = "default",
    message: str | None = None,
    semantic_config: Path | None = None,
    llm_provider: Literal["gemini", "mock"] = "mock",
    llm_model: str = "gemini-2.5-flash",
    llm_api_key_env: str = "GEMINI_API_KEY",
    llm_temperature: float = 0.0,
) -> None:
    conversation = build_conversation_agent(
        db_url,
        semantic_config,
        llm_provider=llm_provider,
        llm_model=llm_model,
        llm_api_key_env=llm_api_key_env,
        llm_temperature=llm_temperature,
    )
    if message is not None:
        typer.echo(conversation.chat(message=message, session_id=session_id).model_dump_json(indent=2))
        return

    typer.echo(f"Starting chat session '{session_id}'. Type 'exit' to quit.")
    while True:
        user_message = typer.prompt("you").strip()
        if user_message.lower() in {"exit", "quit"}:
            break
        response = conversation.chat(message=user_message, session_id=session_id)
        typer.echo(f"assistant: {response.message}")
        if response.notes_path:
            typer.echo(f"notes: {response.notes_path}")
        if response.sql:
            typer.echo(f"sql: {response.sql}")
