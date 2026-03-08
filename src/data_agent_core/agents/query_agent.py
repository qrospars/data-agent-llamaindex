from __future__ import annotations

from collections.abc import Callable

from data_agent_core.config.loader import load_semantic_config
from data_agent_core.config.models import AppConfig
from data_agent_core.connectors.sqlalchemy_connector import SQLAlchemyConnector
from data_agent_core.llm.factory import create_llm
from data_agent_core.output.chart_suggester import ChartSuggester
from data_agent_core.output.models import AgentResponse, QueryExecutionResult, SQLGenerationResult
from data_agent_core.output.summarizer import AnswerSummarizer
from data_agent_core.schema.introspection import SchemaIntrospector
from data_agent_core.sql.executor import SafeSQLExecutor
from data_agent_core.sql.formatter import ensure_limit
from data_agent_core.sql.generator import SQLGenerator
from data_agent_core.sql.rewriter import normalize_sql
from data_agent_core.sql.validator import SQLValidator

ProgressCallback = Callable[[str, str], None]


def _emit_progress(progress_callback: ProgressCallback | None, stage: str, message: str) -> None:
    if progress_callback is None:
        return
    try:
        progress_callback(stage, message)
    except Exception:
        # Progress hooks are best-effort and must never break query execution.
        return


class QueryAgent:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.semantic = load_semantic_config(config.semantic_config_path, config.semantic_config_data)
        self.connector = SQLAlchemyConnector(config.db_url, config.query_timeout_seconds)
        self.validator = SQLValidator(
            forbidden_tables=[*config.forbidden_tables, *self.semantic.forbidden_tables],
            allowed_tables=config.allowed_tables,
            max_row_limit=config.max_row_limit,
        )
        self.executor = SafeSQLExecutor(self.connector, truncate_rows=config.max_row_limit)
        self.llm = create_llm(config.llm_provider)
        self.summarizer = AnswerSummarizer(
            llm=self.llm,
            semantic=self.semantic,
            enable_llm_summary=config.llm_provider.provider != "mock",
        )
        self.chart_suggester = ChartSuggester()

    def generate_sql(
        self,
        question: str,
        progress_callback: ProgressCallback | None = None,
    ) -> SQLGenerationResult:
        _emit_progress(progress_callback, "schema.inspect", "Inspecting schema and semantic context")
        context = SchemaIntrospector(self.connector, self.semantic).build_context()
        _emit_progress(progress_callback, "sql.generate", "Generating SQL from your question")
        generator = SQLGenerator(self.llm, default_row_limit=self.config.default_row_limit)
        generated = generator.generate(question, context.minimal_schema_context, self.semantic)
        _emit_progress(progress_callback, "sql.normalize", "Normalizing SQL and applying row limits")
        normalized = normalize_sql(generated.sql)
        with_limit, _, limit_warnings = ensure_limit(
            normalized,
            row_limit=self.config.default_row_limit,
            max_row_limit=self.config.max_row_limit,
        )
        _emit_progress(progress_callback, "sql.validate", "Validating SQL safety rules")
        validation = self.validator.validate(with_limit)
        generated.sql = with_limit
        generated.normalized_sql = with_limit
        generated.validation_passed = validation.passed
        generated.validation_errors = [*validation.errors, *limit_warnings]
        return generated

    def run_sql(self, sql: str) -> QueryExecutionResult:
        generated = normalize_sql(sql)
        with_limit, _, _ = ensure_limit(
            generated,
            row_limit=self.config.default_row_limit,
            max_row_limit=self.config.max_row_limit,
        )
        validated = self.validator.validate(with_limit)
        if not validated.passed:
            raise ValueError(f"Unsafe SQL: {validated.errors}")
        return self.executor.execute(with_limit)

    def explain(
        self,
        question: str,
        sql: str,
        result: QueryExecutionResult,
        progress_callback: ProgressCallback | None = None,
    ) -> str:
        _emit_progress(progress_callback, "answer.summarize", "Summarizing the query result")
        return self.summarizer.summarize(question, result, sql)

    def ask(
        self,
        question: str,
        progress_callback: ProgressCallback | None = None,
    ) -> AgentResponse:
        generation = self.generate_sql(question, progress_callback=progress_callback)
        if not generation.validation_passed:
            raise ValueError(f"Generated SQL failed validation: {generation.validation_errors}")
        _emit_progress(progress_callback, "sql.execute", "Executing SQL against the database")
        result = self.executor.execute(generation.sql)
        summary = self.explain(question, generation.sql, result, progress_callback=progress_callback)
        _emit_progress(progress_callback, "answer.chart", "Selecting chart type from result shape")
        chart = self.chart_suggester.suggest(result)
        return AgentResponse(
            question=question,
            sql=generation.sql,
            columns=result.columns,
            rows=result.rows,
            row_count=result.row_count,
            duration_ms=result.duration_ms,
            summary=summary,
            chart_suggestion=chart,
            warnings=[],
        )
