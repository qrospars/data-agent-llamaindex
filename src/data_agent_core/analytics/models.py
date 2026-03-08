from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class ColumnSchema(BaseModel):
    name: str
    duckdb_type: str
    nullable: bool | None = None
    sample_values: list[str] = Field(default_factory=list)
    inferred_role: Literal["measure", "dimension", "time", "id", "unknown"] = "unknown"


class TableSchema(BaseModel):
    table_name: str
    columns: list[ColumnSchema]


class IngestResult(BaseModel):
    dataset_name: str
    table_name: str
    row_count: int
    schema: TableSchema
    created_at: datetime


class ArrowTablePreview(BaseModel):
    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int
    truncated: bool = False


class NumericProfile(BaseModel):
    column: str
    min: float | None = None
    max: float | None = None
    mean: float | None = None
    median: float | None = None
    stddev: float | None = None


class ColumnProfile(BaseModel):
    column: str
    null_rate: float
    distinct_count: int | None = None
    top_values: list[tuple[str, int]] = Field(default_factory=list)


class DatasetProfile(BaseModel):
    dataset_name: str
    row_count: int
    date_range: dict[str, str | None]
    numeric_profiles: list[NumericProfile] = Field(default_factory=list)
    column_profiles: list[ColumnProfile] = Field(default_factory=list)
    candidate_time_columns: list[str] = Field(default_factory=list)
    candidate_dimension_columns: list[str] = Field(default_factory=list)
    candidate_measure_columns: list[str] = Field(default_factory=list)


class SemanticMetric(BaseModel):
    name: str
    sql: str
    description: str
    confidence: float
    caveats: list[str] = Field(default_factory=list)


class SemanticDimension(BaseModel):
    name: str
    column: str
    grain: str | None = None
    description: str
    confidence: float


class SemanticModel(BaseModel):
    dataset_name: str
    table_name: str
    metrics: list[SemanticMetric] = Field(default_factory=list)
    dimensions: list[SemanticDimension] = Field(default_factory=list)
    business_rules: list[str] = Field(default_factory=list)
    preferred_time_dimension: str | None = None


class AnalysisRequest(BaseModel):
    dataset_name: str
    question: str
    analysis_mode: Literal["overview", "executive_summary", "diagnostic", "deep_dive"]


class AnalysisTask(BaseModel):
    module: Literal["overview", "trends", "segments", "anomalies", "diagnostics"]
    metric: str | None = None
    dimension: str | None = None
    grain: str | None = None
    priority: int


class AnalysisPlan(BaseModel):
    dataset_name: str
    goal: str
    tasks: list[AnalysisTask] = Field(default_factory=list)
    reasoning_summary: str


class EvidenceRef(BaseModel):
    module: str
    query_id: str
    table_name: str


class Finding(BaseModel):
    title: str
    observation: str
    interpretation: str | None = None
    metric: str | None = None
    dimensions: list[str] = Field(default_factory=list)
    impact_score: float
    confidence_score: float
    business_relevance_score: float
    severity: Literal["low", "medium", "high"]
    caveats: list[str] = Field(default_factory=list)
    evidence: list[EvidenceRef] = Field(default_factory=list)


class EvidenceBundle(BaseModel):
    dataset_name: str
    question: str
    findings: list[Finding] = Field(default_factory=list)
    raw_module_outputs: dict[str, Any] = Field(default_factory=dict)


class ExecutiveSummary(BaseModel):
    headline: str
    top_findings: list[str] = Field(default_factory=list)
    key_risks: list[str] = Field(default_factory=list)
    caveats: list[str] = Field(default_factory=list)
    follow_up_questions: list[str] = Field(default_factory=list)


class RegisteredDataset(BaseModel):
    dataset_name: str
    table_name: str
    source_path: str
    created_at: datetime
    row_count: int
    semantic_path: str | None = None
    profile_path: str | None = None


class RunAnalysisInput(BaseModel):
    dataset_name: str
    question: str
    analysis_mode: str


class RunAnalysisOutput(BaseModel):
    plan: AnalysisPlan
    evidence_bundle: EvidenceBundle
    executive_summary: ExecutiveSummary
