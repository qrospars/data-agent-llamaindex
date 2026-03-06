from __future__ import annotations

from dataclasses import dataclass

from data_agent_core.connectors.base import DatabaseConnector
from data_agent_core.config.models import SemanticConfig


@dataclass
class SchemaContext:
    full_schema_context: str
    minimal_schema_context: str
    table_map: dict[str, list[str]]
    metric_map: dict[str, str]


class SchemaIntrospector:
    def __init__(self, connector: DatabaseConnector, semantic: SemanticConfig) -> None:
        self.connector = connector
        self.semantic = semantic

    def build_context(self) -> SchemaContext:
        tables = self.connector.list_tables()
        views = self.connector.list_views()
        table_map: dict[str, list[str]] = {}

        for name in [*tables, *views]:
            columns = self.connector.get_columns(name)
            table_map[name] = [column["name"] for column in columns]

        metric_map = {item.get("name", ""): item.get("description", "") for item in self.semantic.metrics}

        lines = ["Database objects:"]
        for object_name, columns in table_map.items():
            lines.append(f"- {object_name}: {', '.join(columns)}")

        if self.semantic.business_rules:
            lines.append("Business rules:")
            lines.extend([f"- {rule}" for rule in self.semantic.business_rules])

        full = "\n".join(lines)
        minimal = "\n".join(lines[: min(12, len(lines))])
        return SchemaContext(full, minimal, table_map, metric_map)
