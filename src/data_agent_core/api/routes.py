from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter
from pydantic import BaseModel

from data_agent_core.agents.query_agent import QueryAgent
from data_agent_core.config.models import AppConfig

router = APIRouter()


class AskRequest(BaseModel):
    question: str
    db_url: str
    semantic_config_path: str | None = None


class SQLRequest(BaseModel):
    sql: str
    db_url: str
    semantic_config_path: str | None = None


def _agent(db_url: str, semantic_path: str | None) -> QueryAgent:
    config = AppConfig(
        db_url=db_url,
        semantic_config_path=Path(semantic_path) if semantic_path else None,
    )
    return QueryAgent(config)


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.post("/ask")
def ask(payload: AskRequest) -> dict[str, object]:
    return _agent(payload.db_url, payload.semantic_config_path).ask(payload.question).model_dump()


@router.post("/generate-sql")
def generate_sql(payload: AskRequest) -> dict[str, object]:
    return _agent(payload.db_url, payload.semantic_config_path).generate_sql(payload.question).model_dump()


@router.post("/run-sql")
def run_sql(payload: SQLRequest) -> dict[str, object]:
    return _agent(payload.db_url, payload.semantic_config_path).run_sql(payload.sql).model_dump()
