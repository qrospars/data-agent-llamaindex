from __future__ import annotations

import statistics
from typing import Any

from data_agent_core.analytics.models import EvidenceRef, Finding
from data_agent_core.analytics.modules._base import ModuleContext


_ALLOWED_GRAINS = {"day", "week", "month"}


class AnomaliesModule:
    def __init__(self, context: ModuleContext | None = None) -> None:
        self.context = context or ModuleContext()

    def run(self, dataset_name: str, metric: str, grain: str = "week") -> dict[str, Any]:
        if grain not in _ALLOWED_GRAINS:
            raise ValueError(f"Unsupported grain: {grain}")

        semantic_model = self.context.load_semantic_model(dataset_name)
        metric_def = next((item for item in semantic_model.metrics if item.name == metric), None)
        if metric_def is None:
            raise ValueError(f"Unknown metric: {metric}")
        if not semantic_model.preferred_time_dimension:
            raise ValueError("No preferred_time_dimension configured")

        table_name = self.context.resolve_table(dataset_name)
        runtime = self.context.runtime_for(dataset_name)
        time_col = semantic_model.preferred_time_dimension
        query_id = f"anomalies_{metric}_{grain}"

        rows = runtime.execute_rows(
            f"SELECT DATE_TRUNC('{grain}', CAST({time_col} AS TIMESTAMP)) AS period, "
            f"{metric_def.sql} AS metric_value FROM {table_name} GROUP BY 1 ORDER BY 1",
            row_limit=5000,
        )
        values = [float(item["metric_value"] or 0.0) for item in rows]

        anomalies: list[dict[str, Any]] = []
        if len(values) >= 4:
            mean = statistics.fmean(values)
            std = statistics.pstdev(values)
            median = statistics.median(values)
            abs_dev = [abs(value - median) for value in values]
            mad = statistics.median(abs_dev) if abs_dev else 0.0

            for idx, value in enumerate(values):
                z_score = (value - mean) / std if std else 0.0
                mad_score = (0.6745 * (value - median) / mad) if mad else 0.0
                if abs(z_score) >= 2.5 or abs(mad_score) >= 3.5:
                    prev = values[idx - 1] if idx > 0 else None
                    abs_delta = (value - prev) if prev is not None else None
                    rel_delta = ((value - prev) / prev) if prev not in (None, 0) else None
                    anomalies.append(
                        {
                            "period": str(rows[idx]["period"]),
                            "value": value,
                            "z_score": z_score,
                            "mad_score": mad_score,
                            "absolute_delta": abs_delta,
                            "relative_delta": rel_delta,
                        }
                    )

        findings: list[Finding] = []
        if anomalies:
            persistent = len(anomalies) >= 2
            findings.append(
                Finding(
                    title=f"Anomaly candidates detected for {metric}",
                    observation=f"Detected {len(anomalies)} statistical anomaly candidate periods.",
                    interpretation=(
                        "Pattern appears persistent across multiple periods."
                        if persistent
                        else "Pattern appears one-off and should be verified."
                    ),
                    metric=metric,
                    dimensions=[time_col],
                    impact_score=min(1.0, len(anomalies) / max(1, len(values))),
                    confidence_score=0.68,
                    business_relevance_score=0.81,
                    severity="high" if persistent else "medium",
                    caveats=["Statistical anomaly does not imply root cause."],
                    evidence=[EvidenceRef(module="anomalies", query_id=query_id, table_name=table_name)],
                )
            )
        else:
            findings.append(
                Finding(
                    title=f"No strong anomaly signals for {metric}",
                    observation="No periods exceeded configured z-score/MAD thresholds.",
                    interpretation="Series looks relatively stable at selected grain.",
                    metric=metric,
                    dimensions=[time_col],
                    impact_score=0.2,
                    confidence_score=0.72,
                    business_relevance_score=0.55,
                    severity="low",
                    caveats=["Threshold-based detection can miss subtle anomalies."],
                    evidence=[EvidenceRef(module="anomalies", query_id=query_id, table_name=table_name)],
                )
            )

        return {
            "findings": findings,
            "raw_module_outputs": {
                "series": rows,
                "anomalies": anomalies,
            },
        }
