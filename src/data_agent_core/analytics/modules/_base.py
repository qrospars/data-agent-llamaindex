from __future__ import annotations

import json
from pathlib import Path

import yaml

from data_agent_core.analytics.models import SemanticModel
from data_agent_core.analytics.runtime import AnalyticsSQLRuntime
from data_agent_core.storage.dataset_registry import DatasetRegistry
from data_agent_core.storage.duckdb_loader import DuckDBLoader


class ModuleContext:
    def __init__(self, db_path: str | Path = ".state/analytics.duckdb", state_root: str | Path = ".state") -> None:
        self.loader = DuckDBLoader(db_path=db_path)
        self.registry = DatasetRegistry(state_root=state_root)

    def resolve_table(self, dataset_name: str) -> str:
        dataset = self.registry.get_dataset(dataset_name)
        return dataset.table_name

    def runtime_for(self, dataset_name: str) -> AnalyticsSQLRuntime:
        table_name = self.resolve_table(dataset_name)
        return AnalyticsSQLRuntime(
            db_url=self.loader.db_url(),
            allowed_tables=[dataset_name, table_name],
            default_row_limit=1000,
            max_row_limit=10000,
        )

    def load_semantic_model(self, dataset_name: str) -> SemanticModel:
        dataset = self.registry.get_dataset(dataset_name)
        if not dataset.semantic_path:
            raise ValueError(f"Dataset {dataset_name} has no semantic model registered")

        semantic_path = Path(dataset.semantic_path)
        if not semantic_path.exists():
            raise FileNotFoundError(f"Semantic model not found: {semantic_path}")

        raw_text = semantic_path.read_text(encoding="utf-8")
        if semantic_path.suffix.lower() in {".yaml", ".yml"}:
            payload = yaml.safe_load(raw_text) or {}
        else:
            payload = json.loads(raw_text)
        return SemanticModel.model_validate(payload)
