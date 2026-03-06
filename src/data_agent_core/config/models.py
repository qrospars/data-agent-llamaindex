from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


class ProviderConfig(BaseModel):
    provider: Literal["gemini", "mock"] = "mock"
    model: str = "gemini-2.5-flash"
    api_key_env: str = "GEMINI_API_KEY"
    temperature: float = 0.0


class SemanticTable(BaseModel):
    name: str
    description: str = ""
    preferred: bool = False


class SemanticColumn(BaseModel):
    table: str
    name: str
    description: str = ""


class ExamplePair(BaseModel):
    question: str
    sql: str


class SemanticConfig(BaseModel):
    project: str = "default"
    tables: list[SemanticTable] = Field(default_factory=list)
    columns: list[SemanticColumn] = Field(default_factory=list)
    metrics: list[dict[str, str]] = Field(default_factory=list)
    dimensions: list[dict[str, str]] = Field(default_factory=list)
    synonyms: dict[str, list[str]] = Field(default_factory=dict)
    forbidden_tables: list[str] = Field(default_factory=list)
    preferred_views: list[str] = Field(default_factory=list)
    business_rules: list[str] = Field(default_factory=list)
    examples: list[ExamplePair] = Field(default_factory=list)


class AppConfig(BaseModel):
    db_url: str
    db_dialect: str | None = None
    llm_provider: ProviderConfig = Field(default_factory=ProviderConfig)
    semantic_config_path: Path | None = None
    semantic_config_data: dict[str, Any] | None = None
    allowed_tables: list[str] = Field(default_factory=list)
    forbidden_tables: list[str] = Field(default_factory=list)
    default_row_limit: int = 100
    max_row_limit: int = 1000
    query_timeout_seconds: int = 30
    debug: bool = False

    @field_validator("default_row_limit", "max_row_limit")
    @classmethod
    def positive_limit(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("Row limits must be positive integers")
        return value
