from __future__ import annotations

from pathlib import Path

import typer

from data_agent_core.agents.query_agent import QueryAgent
from data_agent_core.config.models import AppConfig

app = typer.Typer(help="data-agent-core CLI")


def build_agent(db_url: str, semantic_config: Path | None) -> QueryAgent:
    config = AppConfig(db_url=db_url, semantic_config_path=semantic_config)
    return QueryAgent(config)


@app.command("ask")
def ask(question: str, db_url: str, semantic_config: Path | None = None) -> None:
    response = build_agent(db_url, semantic_config).ask(question)
    typer.echo(response.model_dump_json(indent=2))


@app.command("sql")
def sql(question: str, db_url: str, semantic_config: Path | None = None) -> None:
    result = build_agent(db_url, semantic_config).generate_sql(question)
    typer.echo(result.model_dump_json(indent=2))


@app.command("run-sql")
def run_sql(sql_text: str, db_url: str, semantic_config: Path | None = None) -> None:
    result = build_agent(db_url, semantic_config).run_sql(sql_text)
    typer.echo(result.model_dump_json(indent=2))


@app.command("inspect")
def inspect(db_url: str, semantic_config: Path | None = None) -> None:
    agent = build_agent(db_url, semantic_config)
    payload = {
        "tables": agent.connector.list_tables(),
        "views": agent.connector.list_views(),
        "dialect": agent.connector.get_dialect(),
    }
    typer.echo(payload)


@app.command("doctor")
def doctor(db_url: str, semantic_config: Path | None = None) -> None:
    agent = build_agent(db_url, semantic_config)
    checks = {
        "db_connectivity": bool(agent.connector.get_dialect()),
        "semantic_loaded": bool(agent.semantic.project),
        "llm_provider": agent.config.llm_provider.provider,
    }
    typer.echo(checks)
