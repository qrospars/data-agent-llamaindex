from __future__ import annotations

import json
from pathlib import Path

import yaml

from data_agent_core.analytics.models import SemanticModel, TableSchema
from data_agent_core.analytics.profiler import DatasetProfiler
from data_agent_core.analytics.semantic_inference import SemanticInferenceService
from data_agent_core.storage.dataset_registry import DatasetRegistry
from data_agent_core.storage.duckdb_loader import DuckDBLoader


def analytics_profile_dataset(
    dataset_name: str,
    state_root: str = ".state",
    db_path: str = ".state/analytics.duckdb",
) -> dict[str, object]:
    registry = DatasetRegistry(state_root=state_root)
    dataset = registry.get_dataset(dataset_name)
    profiler = DatasetProfiler(db_path=db_path, state_root=state_root)
    profile = profiler.profile_dataset(dataset_name=dataset_name, table_name=dataset.table_name)
    return profile.model_dump(mode="json")


def analytics_build_semantic_layer(
    dataset_name: str,
    business_context: str | None = None,
    state_root: str = ".state",
    db_path: str = ".state/analytics.duckdb",
) -> dict[str, object]:
    registry = DatasetRegistry(state_root=state_root)
    dataset = registry.get_dataset(dataset_name)

    profiler = DatasetProfiler(db_path=db_path, state_root=state_root)
    profile = profiler.profile_dataset(dataset_name=dataset_name, table_name=dataset.table_name)

    loader = DuckDBLoader(db_path=db_path)
    schema = TableSchema(table_name=dataset.table_name, columns=loader.get_schema(dataset.table_name))

    service = SemanticInferenceService(state_root=state_root)
    semantic_model = service.build_semantic_model(
        profile=profile,
        schema=schema,
        business_context=business_context,
    )

    semantic_path = registry.semantics_dir / f"{dataset_name}.json"
    return {
        "dataset_name": dataset_name,
        "semantic_model_path": str(semantic_path),
        "semantic_model": semantic_model.model_dump(mode="json"),
    }


def analytics_explain_metric(
    dataset_name: str,
    metric_name: str,
    state_root: str = ".state",
) -> dict[str, object]:
    registry = DatasetRegistry(state_root=state_root)
    dataset = registry.get_dataset(dataset_name)
    if not dataset.semantic_path:
        raise ValueError(f"Dataset {dataset_name} has no semantic model")

    path = Path(dataset.semantic_path)
    raw = path.read_text(encoding="utf-8")
    if path.suffix.lower() in {".yaml", ".yml"}:
        payload = yaml.safe_load(raw) or {}
    else:
        payload = json.loads(raw)

    semantic_model = SemanticModel.model_validate(payload)
    metric = next((item for item in semantic_model.metrics if item.name == metric_name), None)
    if metric is None:
        raise ValueError(f"Unknown metric: {metric_name}")

    common_dimensions = [
        dimension.name
        for dimension in semantic_model.dimensions
        if dimension.name != semantic_model.preferred_time_dimension
    ][:5]

    return {
        "metric_name": metric.name,
        "description": metric.description,
        "sql": metric.sql,
        "dimensions_commonly_used": common_dimensions,
        "caveats": metric.caveats,
    }
