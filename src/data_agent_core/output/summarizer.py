from __future__ import annotations

from data_agent_core.output.models import QueryExecutionResult


class AnswerSummarizer:
    def summarize(self, question: str, result: QueryExecutionResult) -> str:
        if result.row_count == 0:
            return f"No rows returned for question: {question}"
        return f"Returned {result.row_count} rows in {result.duration_ms} ms."
