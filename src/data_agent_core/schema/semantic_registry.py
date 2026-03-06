from __future__ import annotations

from data_agent_core.config.loader import load_semantic_config
from data_agent_core.config.models import SemanticConfig


class SemanticRegistry:
    def __init__(self, semantic: SemanticConfig) -> None:
        self.semantic = semantic

    @classmethod
    def from_path(cls, path: str) -> "SemanticRegistry":
        semantic = load_semantic_config(path=__import__("pathlib").Path(path))
        return cls(semantic)

    def preferred_objects(self) -> list[str]:
        preferred_tables = [table.name for table in self.semantic.tables if table.preferred]
        return list(dict.fromkeys([*self.semantic.preferred_views, *preferred_tables]))
