from __future__ import annotations

import statistics
from typing import Any

from data_agent_core.analytics.models import EvidenceRef, Finding
from data_agent_core.analytics.modules._base import ModuleContext


_ALLOWED_GRAINS = {"day", "week", "month", "quarter", "year"}


class TrendsModule:
    def __init__(self, context: ModuleContext | None = None) -> None:
        self.context = context or ModuleContext()

    def run(self, dataset_name: str, metric: str, grain: str = "month") -> dict[str, Any]:
        if grain not in _ALLOWED_GRAINS:
            raise ValueError(f"Unsupported grain: {grain}")

        semantic_model = self.context.load_semantic_model(dataset_name)
        metric_def = next((item for item in semantic_model.metrics if item.name == metric), None)
        if metric_def is None:
            raise ValueError(f"Unknown metric: {metric}")
        if not semantic_model.preferred_time_dimension:
            raise ValueError("No preferred_time_dimension configured")

        time_col = semantic_model.preferred_time_dimension
        table_name = self.context.resolve_table(dataset_name)
        runtime = self.context.runtime_for(dataset_name)
        query_id = f"trends_{metric}_{grain}"
        rows = runtime.execute_rows(
            f"SELECT DATE_TRUNC('{grain}', CAST({time_col} AS TIMESTAMP)) AS period, "
            f"{metric_def.sql} AS metric_value FROM {table_name} "
            "GROUP BY 1 ORDER BY 1",
            row_limit=5000,
        )

        values = [float(item["metric_value"] or 0.0) for item in rows]
        deltas: list[dict[str, Any]] = []
        for index in range(1, len(rows)):
            prev = values[index - 1]
            cur = values[index]
            delta = cur - prev
            rel = (delta / prev) if prev else None
            deltas.append(
                {
                    "period": str(rows[index]["period"]),
                    "delta": delta,
                    "relative_delta": rel,
                }
            )

        findings: list[Finding] = []
        if deltas:
            max_increase = max(deltas, key=lambda item: item["delta"])
            max_decrease = min(deltas, key=lambda item: item["delta"])
            findings.append(
                Finding(
                    title=f"Strongest increase in {metric}",
                    observation=(
                        f"Largest period gain is {max_increase['delta']:.2f} on {max_increase['period']}"
                    ),
                    interpretation="Largest positive period-over-period movement.",
                    metric=metric,
                    dimensions=[time_col],
                    impact_score=min(1.0, abs(max_increase["delta"]) / (abs(values[-1]) + 1.0)),
                    confidence_score=0.78,
                    business_relevance_score=0.82,
                    severity="medium",
                    caveats=[],
                    evidence=[EvidenceRef(module="trends", query_id=query_id, table_name=table_name)],
                )
            )
            findings.append(
                Finding(
                    title=f"Strongest decrease in {metric}",
                    observation=(
                        f"Largest period decline is {max_decrease['delta']:.2f} on {max_decrease['period']}"
                    ),
                    interpretation="Largest negative period-over-period movement.",
                    metric=metric,
                    dimensions=[time_col],
                    impact_score=min(1.0, abs(max_decrease["delta"]) / (abs(values[-1]) + 1.0)),
                    confidence_score=0.78,
                    business_relevance_score=0.82,
                    severity="high" if max_decrease["delta"] < 0 else "low",
                    caveats=[],
                    evidence=[EvidenceRef(module="trends", query_id=query_id, table_name=table_name)],
                )
            )

        volatility = statistics.pstdev(values) if len(values) > 1 else 0.0
        avg = statistics.fmean(values) if values else 0.0
        volatility_ratio = (volatility / abs(avg)) if avg else 0.0
        findings.append(
            Finding(
                title=f"{metric} volatility",
                observation=f"Volatility ratio is {volatility_ratio:.2f} at {grain} grain",
                interpretation="Higher ratios indicate unstable trend behavior.",
                metric=metric,
                dimensions=[time_col],
                impact_score=min(1.0, volatility_ratio),
                confidence_score=0.74,
                business_relevance_score=0.7,
                severity="high" if volatility_ratio > 0.5 else "medium",
                caveats=["Computed from visible time series only."],
                evidence=[EvidenceRef(module="trends", query_id=query_id, table_name=table_name)],
            )
        )

        spikes: list[dict[str, Any]] = []
        if len(values) > 2 and volatility > 0:
            mean = statistics.fmean(values)
            for idx, value in enumerate(values):
                z = (value - mean) / volatility
                if abs(z) >= 2.0:
                    spikes.append({"period": str(rows[idx]["period"]), "value": value, "z_score": z})
            if spikes:
                findings.append(
                    Finding(
                        title=f"Spike/drop flags for {metric}",
                        observation=f"Detected {len(spikes)} anomaly candidate periods.",
                        interpretation="Potential change-points requiring diagnostic follow-up.",
                        metric=metric,
                        dimensions=[time_col],
                        impact_score=min(1.0, len(spikes) / max(1, len(values))),
                        confidence_score=0.69,
                        business_relevance_score=0.76,
                        severity="medium",
                        caveats=["Anomaly flag is statistical, not causal."],
                        evidence=[EvidenceRef(module="trends", query_id=query_id, table_name=table_name)],
                    )
                )

        return {
            "findings": findings,
            "raw_module_outputs": {
                "series": rows,
                "deltas": deltas,
                "volatility_ratio": volatility_ratio,
                "spike_flags": spikes,
            },
        }
