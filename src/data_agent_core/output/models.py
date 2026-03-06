from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class SQLGenerationResult(BaseModel):
    question: str
    sql: str
    raw_llm_output: str
    normalized_sql: str
    validation_passed: bool
    validation_errors: list[str] = Field(default_factory=list)


class QueryExecutionResult(BaseModel):
    sql: str
    columns: list[str]
    rows: list[list[Any]]
    row_count: int
    duration_ms: int
    truncated: bool


class AgentResponse(BaseModel):
    question: str
    sql: str
    columns: list[str]
    rows: list[list[Any]]
    row_count: int
    duration_ms: int
    summary: str
    chart_suggestion: str | None
    warnings: list[str] = Field(default_factory=list)


class ConversationTurn(BaseModel):
    user_message: str
    assistant_message: str
    mode: Literal["query", "chat", "meta", "error"]
    sql: str | None = None


class ConversationResponse(BaseModel):
    session_id: str
    mode: Literal["query", "chat", "meta", "error"]
    message: str
    sql: str | None = None
    columns: list[str] = Field(default_factory=list)
    rows: list[list[Any]] = Field(default_factory=list)
    row_count: int = 0
    chart_suggestion: str | None = None
    notes_path: str | None = None
