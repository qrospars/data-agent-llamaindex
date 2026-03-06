from __future__ import annotations

from typing import Any

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
