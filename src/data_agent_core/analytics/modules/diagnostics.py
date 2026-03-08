from __future__ import annotations

from typing import Any

from data_agent_core.analytics.models import EvidenceRef, Finding
from data_agent_core.analytics.modules._base import ModuleContext


class DiagnosticsModule:
    def __init__(self, context: ModuleContext | None = None) -> None:
        self.context = context or ModuleContext()

    def run(self, dataset_name: str) -> dict[str, Any]:
        semantic_model = self.context.load_semantic_model(dataset_name)
        table_name = self.context.resolve_table(dataset_name)
        runtime = self.context.runtime_for(dataset_name)
        schema = self.context.loader.get_schema(table_name)

        findings: list[Finding] = []
        raw_outputs: dict[str, Any] = {
            "missingness": [],
            "sparse_dimensions": [],
            "category_instability": [],
            "duplicate_keys": None,
        }

        row_info = runtime.execute_rows(f"SELECT COUNT(*) AS row_count FROM {table_name}")
        row_count = int(row_info[0]["row_count"] if row_info else 0)

        missingness_rows: list[dict[str, Any]] = []
        for column in schema:
            rows = runtime.execute_rows(
                f"SELECT SUM(CASE WHEN {column.name} IS NULL THEN 1 ELSE 0 END) AS null_count "
                f"FROM {table_name}"
            )
            null_count = int(rows[0]["null_count"] or 0)
            null_rate = (null_count / row_count) if row_count else 0.0
            missingness_rows.append(
                {
                    "column": column.name,
                    "null_count": null_count,
                    "null_rate": null_rate,
                }
            )
        raw_outputs["missingness"] = missingness_rows

        worst_missing = sorted(missingness_rows, key=lambda item: item["null_rate"], reverse=True)[:3]
        if worst_missing and worst_missing[0]["null_rate"] > 0.2:
            findings.append(
                Finding(
                    title="Data missingness risk",
                    observation=(
                        "Highest null-rate columns: "
                        + ", ".join(
                            f"{item['column']} ({item['null_rate']:.1%})" for item in worst_missing
                        )
                    ),
                    interpretation="High null rates can lower confidence in downstream insights.",
                    metric=None,
                    dimensions=[item["column"] for item in worst_missing],
                    impact_score=min(1.0, worst_missing[0]["null_rate"]),
                    confidence_score=0.85,
                    business_relevance_score=0.8,
                    severity="high" if worst_missing[0]["null_rate"] > 0.4 else "medium",
                    caveats=[],
                    evidence=[EvidenceRef(module="diagnostics", query_id="diagnostics_missingness", table_name=table_name)],
                )
            )

        sparse_dimensions: list[dict[str, Any]] = []
        for dim in semantic_model.dimensions[:10]:
            rows = runtime.execute_rows(
                f"SELECT COUNT(DISTINCT {dim.column}) AS distinct_count FROM {table_name}"
            )
            distinct_count = int(rows[0]["distinct_count"] or 0)
            cardinality_ratio = (distinct_count / row_count) if row_count else 0.0
            sparse_dimensions.append(
                {
                    "dimension": dim.name,
                    "distinct_count": distinct_count,
                    "cardinality_ratio": cardinality_ratio,
                }
            )
        raw_outputs["sparse_dimensions"] = sparse_dimensions

        explosions = [item for item in sparse_dimensions if item["cardinality_ratio"] > 0.8]
        if explosions:
            findings.append(
                Finding(
                    title="Sparse/high-cardinality dimensions",
                    observation=(
                        ", ".join(
                            f"{item['dimension']} ({item['cardinality_ratio']:.1%})" for item in explosions[:3]
                        )
                        + " show near-unique category behavior."
                    ),
                    interpretation="High-cardinality dimensions can create unstable segmentation outputs.",
                    metric=None,
                    dimensions=[item["dimension"] for item in explosions[:3]],
                    impact_score=min(1.0, max(item["cardinality_ratio"] for item in explosions)),
                    confidence_score=0.72,
                    business_relevance_score=0.7,
                    severity="medium",
                    caveats=[],
                    evidence=[EvidenceRef(module="diagnostics", query_id="diagnostics_sparse_dims", table_name=table_name)],
                )
            )

        if semantic_model.preferred_time_dimension and semantic_model.dimensions:
            time_col = semantic_model.preferred_time_dimension
            instability: list[dict[str, Any]] = []
            for dim in semantic_model.dimensions[:5]:
                rows = runtime.execute_rows(
                    f"SELECT DATE_TRUNC('month', CAST({time_col} AS TIMESTAMP)) AS period, "
                    f"COUNT(DISTINCT {dim.column}) AS distinct_count "
                    f"FROM {table_name} GROUP BY 1 ORDER BY 1",
                    row_limit=200,
                )
                if not rows:
                    continue
                counts = [int(item["distinct_count"] or 0) for item in rows]
                min_count = min(counts)
                max_count = max(counts)
                ratio = (max_count / min_count) if min_count > 0 else float(max_count)
                instability.append({"dimension": dim.name, "explosion_ratio": ratio})
            raw_outputs["category_instability"] = instability

            unstable = [item for item in instability if item["explosion_ratio"] >= 3.0]
            if unstable:
                findings.append(
                    Finding(
                        title="Category explosion risk",
                        observation=(
                            ", ".join(
                                f"{item['dimension']} ({item['explosion_ratio']:.2f}x)"
                                for item in unstable[:3]
                            )
                            + " shows unstable category growth over time."
                        ),
                        interpretation="Can indicate taxonomy drift or noisy identifiers leaking as dimensions.",
                        metric=None,
                        dimensions=[item["dimension"] for item in unstable[:3]],
                        impact_score=min(1.0, max(item["explosion_ratio"] for item in unstable) / 5.0),
                        confidence_score=0.68,
                        business_relevance_score=0.73,
                        severity="medium",
                        caveats=["Explosion ratio is month-to-month heuristic."],
                        evidence=[EvidenceRef(module="diagnostics", query_id="diagnostics_category_explosion", table_name=table_name)],
                    )
                )

        obvious_key = None
        for column in schema:
            lower = column.name.lower()
            if lower == "id" or lower.endswith("_id"):
                obvious_key = column.name
                break

        if obvious_key is not None:
            rows = runtime.execute_rows(
                f"SELECT COUNT(*) - COUNT(DISTINCT {obvious_key}) AS duplicate_count FROM {table_name}"
            )
            duplicate_count = int(rows[0]["duplicate_count"] or 0)
            raw_outputs["duplicate_keys"] = {"key": obvious_key, "duplicate_count": duplicate_count}
            if duplicate_count > 0:
                findings.append(
                    Finding(
                        title="Suspicious duplicates on key candidate",
                        observation=(
                            f"Found {duplicate_count} duplicate rows for key candidate {obvious_key}."
                        ),
                        interpretation="Potential primary-key quality issue.",
                        metric=None,
                        dimensions=[obvious_key],
                        impact_score=min(1.0, duplicate_count / max(1, row_count)),
                        confidence_score=0.86,
                        business_relevance_score=0.84,
                        severity="high",
                        caveats=[],
                        evidence=[EvidenceRef(module="diagnostics", query_id="diagnostics_duplicates", table_name=table_name)],
                    )
                )

        if not findings:
            findings.append(
                Finding(
                    title="No critical diagnostics alerts",
                    observation="No severe data-quality red flags triggered default heuristics.",
                    interpretation="Dataset passes baseline diagnostic checks.",
                    metric=None,
                    dimensions=[],
                    impact_score=0.2,
                    confidence_score=0.7,
                    business_relevance_score=0.55,
                    severity="low",
                    caveats=["Heuristic diagnostics do not replace full data QA."],
                    evidence=[EvidenceRef(module="diagnostics", query_id="diagnostics_summary", table_name=table_name)],
                )
            )

        return {
            "findings": findings,
            "raw_module_outputs": raw_outputs,
        }
