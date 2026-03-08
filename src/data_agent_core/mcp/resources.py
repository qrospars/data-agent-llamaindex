from __future__ import annotations

from data_agent_core.storage.dataset_registry import DatasetRegistry
from data_agent_core.storage.duckdb_loader import DuckDBLoader


def load_dataset_schema_resource(dataset_name: str, state_root: str = ".state", db_path: str = ".state/analytics.duckdb") -> dict[str, object]:
    registry = DatasetRegistry(state_root=state_root)
    loader = DuckDBLoader(db_path=db_path)
    dataset = registry.get_dataset(dataset_name)
    schema = loader.get_schema(dataset.table_name)
    return {
        "dataset_name": dataset_name,
        "table_name": dataset.table_name,
        "schema": [column.model_dump(mode="json") for column in schema],
    }


def load_dataset_profile_resource(dataset_name: str, state_root: str = ".state") -> dict[str, object]:
    registry = DatasetRegistry(state_root=state_root)
    path = registry.profiles_dir / f"{dataset_name}.json"
    if not path.exists():
        raise FileNotFoundError(f"Profile not found for dataset {dataset_name}")
    return {"dataset_name": dataset_name, "profile": path.read_text(encoding="utf-8")}


def load_dataset_semantic_resource(dataset_name: str, state_root: str = ".state") -> dict[str, object]:
    registry = DatasetRegistry(state_root=state_root)
    path_json = registry.semantics_dir / f"{dataset_name}.json"
    path_yaml = registry.semantics_dir / f"{dataset_name}.yaml"
    path = path_json if path_json.exists() else path_yaml
    if not path.exists():
        raise FileNotFoundError(f"Semantic model not found for dataset {dataset_name}")
    return {
        "dataset_name": dataset_name,
        "semantic_model_path": str(path),
        "semantic_model": path.read_text(encoding="utf-8"),
    }


def load_latest_evidence_resource(dataset_name: str, state_root: str = ".state") -> dict[str, object]:
    registry = DatasetRegistry(state_root=state_root)
    path = registry.evidence_dir / f"{dataset_name}_latest.json"
    if not path.exists():
        raise FileNotFoundError(f"Evidence bundle not found for dataset {dataset_name}")
    return {
        "dataset_name": dataset_name,
        "evidence_bundle": path.read_text(encoding="utf-8"),
    }
