from __future__ import annotations

from data_agent_core.config.models import SemanticConfig
from data_agent_core.llm.prompts import build_sql_prompt
from data_agent_core.output.models import SQLGenerationResult
from data_agent_core.sql.rewriter import normalize_sql


class SQLGenerator:
    def __init__(self, llm: object, default_row_limit: int = 100) -> None:
        self.llm = llm
        self.default_row_limit = default_row_limit

    def generate(self, question: str, schema_context: str, semantic: SemanticConfig) -> SQLGenerationResult:
        prompt = build_sql_prompt(question, schema_context, semantic, self.default_row_limit)
        completion = self.llm.complete(prompt)
        text = completion.text if hasattr(completion, "text") else str(completion)
        sql = normalize_sql(text)
        return SQLGenerationResult(
            question=question,
            sql=sql,
            raw_llm_output=text,
            normalized_sql=sql,
            validation_passed=False,
            validation_errors=[],
        )
