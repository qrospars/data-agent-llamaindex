from data_agent_core.config.models import SemanticConfig


def build_sql_prompt(question: str, schema_context: str, semantic: SemanticConfig, row_limit: int) -> str:
    rules = "\n".join([f"- {rule}" for rule in semantic.business_rules]) or "- Use only valid tables/columns"
    return (
        "You are a SQL generation assistant. Output SQL only.\n"
        f"Question: {question}\n"
        f"Row limit: {row_limit}\n"
        f"Schema:\n{schema_context}\n"
        f"Rules:\n{rules}\n"
    )


def build_summary_prompt(question: str, sql: str, rows_preview: list[list[object]]) -> str:
    return (
        "Summarize SQL result in 2-4 sentences and mention caveats.\n"
        f"Question: {question}\nSQL: {sql}\nPreview: {rows_preview}"
    )
