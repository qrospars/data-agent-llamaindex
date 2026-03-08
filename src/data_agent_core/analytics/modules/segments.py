from __future__ import annotations

from typing import Any

from data_agent_core.analytics.models import EvidenceRef, Finding
from data_agent_core.analytics.modules._base import ModuleContext


class SegmentsModule:
    def __init__(self, context: ModuleContext | None = None) -> None:
        self.context = context or ModuleContext()

    def run(self, dataset_name: str, metric: str, dimension: str) -> dict[str, Any]:
        semantic_model = self.context.load_semantic_model(dataset_name)
        metric_def = next((item for item in semantic_model.metrics if item.name == metric), None)
        if metric_def is None:
            raise ValueError(f"Unknown metric: {metric}")

        dimension_def = next(
            (item for item in semantic_model.dimensions if item.name == dimension or item.column == dimension),
            None,
        )
        if dimension_def is None:
            raise ValueError(f"Unknown dimension: {dimension}")

        table_name = self.context.resolve_table(dataset_name)
        runtime = self.context.runtime_for(dataset_name)
        dim_col = dimension_def.column
        query_id = f"segments_{metric}_{dim_col}"
        rows = runtime.execute_rows(
            f"SELECT {dim_col} AS segment, {metric_def.sql} AS metric_value "
            f"FROM {table_name} GROUP BY 1 ORDER BY 2 DESC",
            row_limit=200,
        )

        metric_values = [float(item["metric_value"] or 0.0) for item in rows]
        total = sum(metric_values)
        top_segments = rows[:10]
        top10_total = sum(float(item["metric_value"] or 0.0) for item in top_segments)
        long_tail_share = ((total - top10_total) / total) if total else 0.0

        concentration_index = 0.0
        if total > 0:
            concentration_index = sum((value / total) ** 2 for value in metric_values)

        findings: list[Finding] = []
        if top_segments:
            winner = top_segments[0]
            loser = top_segments[-1]
            findings.append(
                Finding(
                    title=f"Top segment for {metric}",
                    observation=f"{winner['segment']} leads {metric} with value {winner['metric_value']}",
                    interpretation="Largest contributor segment in current ranking.",
                    metric=metric,
                    dimensions=[dimension_def.name],
                    impact_score=min(1.0, (float(winner["metric_value"] or 0.0) / (total + 1.0))),
                    confidence_score=0.82,
                    business_relevance_score=0.85,
                    severity="medium",
                    caveats=["Based on current grouped aggregation."],
                    evidence=[EvidenceRef(module="segments", query_id=query_id, table_name=table_name)],
                )
            )
            findings.append(
                Finding(
                    title=f"Long-tail share for {dimension_def.name}",
                    observation=f"Long-tail share outside top 10 is {long_tail_share:.1%}",
                    interpretation="Higher share implies less concentration among leaders.",
                    metric=metric,
                    dimensions=[dimension_def.name],
                    impact_score=min(1.0, long_tail_share),
                    confidence_score=0.76,
                    business_relevance_score=0.74,
                    severity="high" if long_tail_share < 0.2 else "low",
                    caveats=[],
                    evidence=[EvidenceRef(module="segments", query_id=query_id, table_name=table_name)],
                )
            )
            findings.append(
                Finding(
                    title=f"Concentration index for {dimension_def.name}",
                    observation=f"HHI-style concentration index is {concentration_index:.3f}",
                    interpretation="Higher index means fewer segments dominate results.",
                    metric=metric,
                    dimensions=[dimension_def.name],
                    impact_score=min(1.0, concentration_index * 4.0),
                    confidence_score=0.7,
                    business_relevance_score=0.72,
                    severity="high" if concentration_index >= 0.25 else "medium",
                    caveats=[],
                    evidence=[EvidenceRef(module="segments", query_id=query_id, table_name=table_name)],
                )
            )
            findings.append(
                Finding(
                    title=f"Winner/loser spread on {dimension_def.name}",
                    observation=(
                        f"Top segment {winner['segment']} exceeds tail segment {loser['segment']} "
                        f"by {(float(winner['metric_value'] or 0.0) - float(loser['metric_value'] or 0.0)):.2f}."
                    ),
                    interpretation="Simple spread view for segment disparity.",
                    metric=metric,
                    dimensions=[dimension_def.name],
                    impact_score=min(
                        1.0,
                        abs(float(winner["metric_value"] or 0.0) - float(loser["metric_value"] or 0.0))
                        / (abs(float(winner["metric_value"] or 0.0)) + 1.0),
                    ),
                    confidence_score=0.72,
                    business_relevance_score=0.79,
                    severity="medium",
                    caveats=["Loser comparison uses last row in returned segment ordering."],
                    evidence=[EvidenceRef(module="segments", query_id=query_id, table_name=table_name)],
                )
            )

        return {
            "findings": findings,
            "raw_module_outputs": {
                "segment_rows": rows,
                "top_segments": top_segments,
                "long_tail_share": long_tail_share,
                "concentration_index": concentration_index,
            },
        }
