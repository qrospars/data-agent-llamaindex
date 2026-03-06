from data_agent_core.config.models import SemanticConfig


def format_examples(semantic: SemanticConfig) -> str:
    if not semantic.examples:
        return ""
    return "\n".join([f"Q: {e.question}\nSQL: {e.sql}" for e in semantic.examples])
