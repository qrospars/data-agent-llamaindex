from __future__ import annotations

from data_agent_core.config.models import SemanticConfig
from data_agent_core.output.models import QueryExecutionResult
from data_agent_core.output.summarizer import AnswerSummarizer


def test_semantic_summary_includes_top_insight() -> None:
    semantic = SemanticConfig(
        metrics=[{"name": "revenue", "description": "Revenue in USD"}],
        dimensions=[{"name": "customer_name", "description": "Customer"}],
    )
    summarizer = AnswerSummarizer(semantic=semantic, enable_llm_summary=False)
    result = QueryExecutionResult(
        sql="SELECT customer_name, revenue FROM sales_summary",
        columns=["customer_name", "revenue"],
        rows=[["Acme", 120.0], ["Blue Harbor", 175.0]],
        row_count=2,
        duration_ms=3,
        truncated=False,
    )

    summary = summarizer.summarize("Top customer by revenue", result, result.sql)

    assert "Top insight" in summary
    assert "revenue" in summary
    assert "customer_name=Blue Harbor" in summary


def test_non_numeric_summary_mentions_caveat() -> None:
    summarizer = AnswerSummarizer(enable_llm_summary=False)
    result = QueryExecutionResult(
        sql="SELECT region FROM customers",
        columns=["region"],
        rows=[["North"], ["South"]],
        row_count=2,
        duration_ms=1,
        truncated=False,
    )

    summary = summarizer.summarize("List regions", result, result.sql)

    assert "non-numeric" in summary


def test_no_rows_summary() -> None:
    summarizer = AnswerSummarizer(enable_llm_summary=False)
    result = QueryExecutionResult(
        sql="SELECT * FROM sales_summary WHERE 1=0",
        columns=["customer_name", "revenue"],
        rows=[],
        row_count=0,
        duration_ms=2,
        truncated=False,
    )

    summary = summarizer.summarize("No data question", result, result.sql)
    assert summary.startswith("No rows returned")
