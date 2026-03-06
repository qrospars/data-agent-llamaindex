from data_agent_core.schema.introspection import SchemaContext


def format_schema_for_prompt(context: SchemaContext, minimal: bool = True) -> str:
    return context.minimal_schema_context if minimal else context.full_schema_context
