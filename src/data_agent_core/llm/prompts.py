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


def build_business_summary_prompt(
    question: str,
    sql: str,
    columns: list[str],
    rows_preview: list[list[object]],
    row_count: int,
    metric_names: list[str],
    dimension_names: list[str],
    business_rules: list[str],
) -> str:
    metrics_text = ", ".join(metric_names) if metric_names else "none"
    dimensions_text = ", ".join(dimension_names) if dimension_names else "none"
    rules_text = "\n".join([f"- {rule}" for rule in business_rules]) or "- none"
    return (
        "You are a business data analyst.\n"
        "Write a concise answer with this structure:\n"
        "1) Direct answer to the user question\n"
        "2) Key business insight from the result\n"
        "3) One caveat or data-quality warning\n"
        "Use only facts from the provided result; do not invent values.\n"
        f"Question: {question}\n"
        f"SQL: {sql}\n"
        f"Columns: {columns}\n"
        f"Row count: {row_count}\n"
        f"Result preview: {rows_preview}\n"
        f"Known metrics: {metrics_text}\n"
        f"Known dimensions: {dimensions_text}\n"
        f"Business rules:\n{rules_text}\n"
    )


def build_intent_prompt(
    message: str,
    recent_turns: list[tuple[str, str]],
    semantic_objects: list[str],
) -> str:
    turns = "\n".join([f"User: {u}\nAssistant: {a}" for u, a in recent_turns]) or "No prior turns."
    objects = ", ".join(semantic_objects) if semantic_objects else "none"
    return (
        "Classify the user intent as exactly one label: QUERY, META, or CHAT.\n"
        "QUERY: asks for analysis/query/calculation/filter/breakdown over data.\n"
        "META: asks about previous SQL/result/history/session behavior.\n"
        "CHAT: greeting/general discussion without a concrete data request.\n"
        "Return only the label.\n"
        f"Semantic objects: {objects}\n"
        f"Conversation:\n{turns}\n"
        f"Current user message: {message}\n"
    )


def build_follow_up_rewrite_prompt(
    message: str,
    previous_question: str,
    previous_sql: str,
    semantic_objects: list[str],
) -> str:
    objects = ", ".join(semantic_objects) if semantic_objects else "none"
    return (
        "Rewrite the follow-up into a standalone analytics question.\n"
        "Keep user intent and constraints. Use database semantics from the objects list.\n"
        "Return only the rewritten question.\n"
        f"Previous question: {previous_question}\n"
        f"Previous SQL: {previous_sql}\n"
        f"Semantic objects: {objects}\n"
        f"Follow-up: {message}\n"
    )
