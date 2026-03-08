from __future__ import annotations

from typing import Any

from data_agent_core.analytics.models import EvidenceRef, Finding, SemanticModel
from data_agent_core.analytics.modules._base import ModuleContext


class OverviewModule:
    def __init__(self, context: ModuleContext | None = None) -> None:
        self.context = context or ModuleContext()

    def run(self, dataset_name: str, semantic_model: SemanticModel) -> dict[str, Any]:
        table_name = self.context.resolve_table(dataset_name)
        runtime = self.context.runtime_for(dataset_name)

        raw_outputs: dict[str, Any] = {"headline_metrics": {}, "date_coverage": {}, "segment_contribution": []}
        findings: list[Finding] = []

        for metric in semantic_model.metrics[:3]:
            query_id = f"overview_metric_{metric.name}"
            rows = runtime.execute_rows(
                f"SELECT {metric.sql} AS metric_value FROM {table_name}",
                row_limit=20,
            )
            value = rows[0]["metric_value"] if rows else None
            raw_outputs["headline_metrics"][metric.name] = value
            findings.append(
                Finding(
                    title=f"Headline metric: {metric.name}",
                    observation=f"Current aggregate value for {metric.name} is {value}",
                    interpretation="High-level baseline metric for executive context.",
                    metric=metric.name,
                    dimensions=[],
                    impact_score=min(1.0, abs(float(value or 0.0)) / 1000.0),
                    confidence_score=0.8,
                    business_relevance_score=0.85,
                    severity="medium",
                    caveats=[] if value is not None else ["Metric returned null"],
                    evidence=[EvidenceRef(module="overview", query_id=query_id, table_name=table_name)],
                )
            )

        if semantic_model.preferred_time_dimension:
            time_col = semantic_model.preferred_time_dimension
            query_id = "overview_date_coverage"
            rows = runtime.execute_rows(
                f"SELECT MIN(CAST({time_col} AS TIMESTAMP)) AS min_date, "
                f"MAX(CAST({time_col} AS TIMESTAMP)) AS max_date FROM {table_name}"
            )
            raw_outputs["date_coverage"] = rows[0] if rows else {}
            if rows:
                findings.append(
                    Finding(
                        title="Dataset date coverage",
                        observation=(
                            f"Data spans from {rows[0].get('min_date')} to {rows[0].get('max_date')}"
                        ),
                        interpretation="Useful for framing trend and seasonality confidence.",
                        metric=None,
                        dimensions=[time_col],
                        impact_score=0.45,
                        confidence_score=0.9,
                        business_relevance_score=0.7,
                        severity="low",
                        caveats=[],
                        evidence=[EvidenceRef(module="overview", query_id=query_id, table_name=table_name)],
                    )
                )

        if semantic_model.metrics and semantic_model.dimensions:
            metric = semantic_model.metrics[0]
            dimension = semantic_model.dimensions[0]
            query_id = f"overview_top_segments_{metric.name}_{dimension.column}"
            rows = runtime.execute_rows(
                f"SELECT {dimension.column} AS segment, {metric.sql} AS metric_value "
                f"FROM {table_name} GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
            )
            raw_outputs["segment_contribution"] = rows
            if rows:
                total = sum(float(item["metric_value"] or 0.0) for item in rows)
                top = float(rows[0]["metric_value"] or 0.0)
                concentration = (top / total) if total else 0.0
                findings.append(
                    Finding(
                        title=f"Top segment concentration on {metric.name}",
                        observation=(
                            f"Top {dimension.name} segment contributes {concentration:.1%} "
                            f"of top-10 visible {metric.name}."
                        ),
                        interpretation="Higher concentration can indicate exposure risk.",
                        metric=metric.name,
                        dimensions=[dimension.name],
                        impact_score=min(1.0, concentration),
                        confidence_score=0.75,
                        business_relevance_score=0.78,
                        severity="high" if concentration >= 0.5 else "medium",
                        caveats=["Contribution uses top-10 segment preview."],
                        evidence=[EvidenceRef(module="overview", query_id=query_id, table_name=table_name)],
                    )
                )

        return {
            "findings": findings,
            "raw_module_outputs": raw_outputs,
        }
