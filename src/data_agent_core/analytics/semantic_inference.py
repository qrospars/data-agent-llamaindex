from __future__ import annotations

import json
import re
from pathlib import Path

import yaml

from data_agent_core.analytics.models import (
    DatasetProfile,
    SemanticDimension,
    SemanticMetric,
    SemanticModel,
    TableSchema,
)
from data_agent_core.config.models import ProviderConfig
from data_agent_core.llm.factory import create_llm
from data_agent_core.sql.validator import SQLValidator
from data_agent_core.storage.dataset_registry import DatasetRegistry


class SemanticInferenceService:
    def __init__(
        self,
        llm_provider: ProviderConfig | None = None,
        state_root: str | Path = ".state",
    ) -> None:
        self.provider = llm_provider or ProviderConfig(provider="mock")
        self.llm = create_llm(self.provider)
        self.registry = DatasetRegistry(state_root=state_root)

    def build_semantic_model(
        self,
        profile: DatasetProfile,
        schema: TableSchema,
        business_context: str | None = None,
    ) -> SemanticModel:
        if self.provider.provider == "mock":
            semantic_model = self._build_deterministic(profile, schema, business_context)
        else:
            semantic_model = self._build_with_llm(profile, schema, business_context)

        self._validate_semantic_model(semantic_model, schema)
        json_path = self.registry.semantics_dir / f"{profile.dataset_name}.json"
        yaml_path = self.registry.semantics_dir / f"{profile.dataset_name}.yaml"
        json_path.write_text(semantic_model.model_dump_json(indent=2), encoding="utf-8")
        yaml_path.write_text(yaml.safe_dump(semantic_model.model_dump(mode="json"), sort_keys=False), encoding="utf-8")
        try:
            self.registry.update_semantic_path(profile.dataset_name, str(yaml_path))
        except KeyError:
            pass
        return semantic_model

    def _build_deterministic(
        self,
        profile: DatasetProfile,
        schema: TableSchema,
        business_context: str | None,
    ) -> SemanticModel:
        metrics: list[SemanticMetric] = []
        for measure in profile.candidate_measure_columns[:5]:
            metrics.append(
                SemanticMetric(
                    name=f"total_{measure}",
                    sql=f"SUM({measure})",
                    description=f"Total {measure}",
                    confidence=0.8,
                    caveats=[],
                )
            )

        dimensions: list[SemanticDimension] = []
        for dim in profile.candidate_dimension_columns[:8]:
            dimensions.append(
                SemanticDimension(
                    name=dim,
                    column=dim,
                    description=f"Dimension based on {dim}",
                    confidence=0.85,
                )
            )
        for time_col in profile.candidate_time_columns[:1]:
            dimensions.append(
                SemanticDimension(
                    name=time_col,
                    column=time_col,
                    grain="day",
                    description=f"Time dimension on {time_col}",
                    confidence=0.9,
                )
            )

        rules = ["Use only known columns", "Prefer additive metrics first"]
        if business_context:
            rules.append(f"Business context: {business_context}")

        return SemanticModel(
            dataset_name=profile.dataset_name,
            table_name=schema.table_name,
            metrics=metrics,
            dimensions=dimensions,
            business_rules=rules,
            preferred_time_dimension=(profile.candidate_time_columns[0] if profile.candidate_time_columns else None),
        )

    def _build_with_llm(
        self,
        profile: DatasetProfile,
        schema: TableSchema,
        business_context: str | None,
    ) -> SemanticModel:
        template = self._prompt_template("semantic_layer.xml")
        prompt = template.format(
            dataset_name=profile.dataset_name,
            table_name=schema.table_name,
            profile_json=profile.model_dump_json(indent=2),
            schema_json=schema.model_dump_json(indent=2),
            business_context=business_context or "",
        )
        completion = self.llm.complete(prompt)
        raw = completion.text if hasattr(completion, "text") else str(completion)
        payload = self._extract_json(raw)
        return SemanticModel.model_validate(payload)

    def _validate_semantic_model(self, semantic_model: SemanticModel, schema: TableSchema) -> None:
        columns = {column.name for column in schema.columns}
        metric_names = {metric.name for metric in semantic_model.metrics}
        validator = SQLValidator(allowed_tables=[schema.table_name])

        for metric in semantic_model.metrics:
            expr = metric.sql.strip()
            match = re.fullmatch(
                r"(?i)(sum|avg|min|max|count)\s*\(\s*(\*|[a-zA-Z_][a-zA-Z0-9_]*)\s*\)",
                expr,
            )
            if match is None:
                raise ValueError(
                    f"Unsafe metric expression for {metric.name}. "
                    "Only SUM/AVG/MIN/MAX/COUNT(column|*) are allowed."
                )
            metric_column = match.group(2)
            if metric_column != "*" and metric_column not in columns:
                raise ValueError(f"Metric {metric.name} references unknown column: {metric_column}")

            metric_sql = f"SELECT {expr} AS metric_value FROM {schema.table_name}"
            validation = validator.validate(metric_sql)
            if not validation.passed:
                raise ValueError(f"Metric SQL for {metric.name} failed safety checks: {validation.errors}")

        for dimension in semantic_model.dimensions:
            if dimension.column not in columns:
                raise ValueError(f"Dimension {dimension.name} references unknown column: {dimension.column}")

        if semantic_model.preferred_time_dimension and semantic_model.preferred_time_dimension not in columns:
            raise ValueError("preferred_time_dimension must reference an existing column")

        # Fail closed when dimensions or metric references are unknown.
        dimension_names = {dimension.name for dimension in semantic_model.dimensions}
        unknown_refs = set()
        for rule in semantic_model.business_rules:
            for token in re.findall(r"\b[a-zA-Z_][a-zA-Z0-9_]*\b", rule):
                if token.startswith("total_") and token not in metric_names:
                    unknown_refs.add(token)
                if token.startswith("dim_") and token[4:] not in dimension_names:
                    unknown_refs.add(token)
        if unknown_refs:
            raise ValueError(f"Unknown semantic references in business rules: {sorted(unknown_refs)}")

    def _extract_json(self, raw_text: str) -> dict[str, object]:
        text = raw_text.strip()
        if text.startswith("```"):
            text = text.strip("`")
            if text.startswith("json"):
                text = text[4:].lstrip()
        return json.loads(text)

    def _prompt_template(self, name: str) -> str:
        prompt_path = Path(__file__).resolve().parent.parent / "prompts" / name
        return prompt_path.read_text(encoding="utf-8")
