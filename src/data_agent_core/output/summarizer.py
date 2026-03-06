from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from data_agent_core.config.models import SemanticConfig
from data_agent_core.llm.prompts import build_business_summary_prompt
from data_agent_core.output.models import QueryExecutionResult


class AnswerSummarizer:
    def __init__(
        self,
        llm: object | None = None,
        semantic: SemanticConfig | None = None,
        enable_llm_summary: bool = False,
        preview_rows: int = 8,
    ) -> None:
        self.llm = llm
        self.semantic = semantic or SemanticConfig()
        self.enable_llm_summary = enable_llm_summary
        self.preview_rows = preview_rows

    def summarize(self, question: str, result: QueryExecutionResult, sql: str = "") -> str:
        if result.row_count == 0:
            return f"No rows returned for question: {question}"

        preview = result.rows[: self.preview_rows]
        metric_names, dimension_names = self._semantic_fields(result.columns)

        if self.enable_llm_summary and self.llm is not None:
            llm_summary = self._llm_summary(question, sql, result.columns, preview, result.row_count)
            if llm_summary:
                return llm_summary

        return self._deterministic_summary(
            question=question,
            columns=result.columns,
            rows=preview,
            row_count=result.row_count,
            duration_ms=result.duration_ms,
            metric_names=metric_names,
            dimension_names=dimension_names,
            truncated=result.truncated,
        )

    def _llm_summary(
        self,
        question: str,
        sql: str,
        columns: list[str],
        rows_preview: list[list[Any]],
        row_count: int,
    ) -> str | None:
        try:
            metric_names, dimension_names = self._semantic_fields(columns)
            prompt = build_business_summary_prompt(
                question=question,
                sql=sql,
                columns=columns,
                rows_preview=rows_preview,
                row_count=row_count,
                metric_names=metric_names,
                dimension_names=dimension_names,
                business_rules=self.semantic.business_rules,
            )
            completion = self.llm.complete(prompt)
            text = completion.text if hasattr(completion, "text") else str(completion)
            clean = text.strip()
            return clean or None
        except Exception:
            return None

    def _semantic_fields(self, columns: Sequence[str]) -> tuple[list[str], list[str]]:
        column_set = {c.lower() for c in columns}
        metrics = []
        dimensions = []

        for metric in self.semantic.metrics:
            name = metric.get("name", "")
            if name and name.lower() in column_set:
                metrics.append(name)

        for dim in self.semantic.dimensions:
            name = dim.get("name", "")
            if name and name.lower() in column_set:
                dimensions.append(name)

        if not metrics:
            metrics = [c for c in columns if self._is_metric_like(c)]
        if not dimensions:
            dimensions = [c for c in columns if c not in metrics]
        return metrics, dimensions

    def _deterministic_summary(
        self,
        question: str,
        columns: list[str],
        rows: list[list[Any]],
        row_count: int,
        duration_ms: int,
        metric_names: list[str],
        dimension_names: list[str],
        truncated: bool,
    ) -> str:
        lead = f"Answer to '{question}': returned {row_count} rows in {duration_ms} ms."
        if not rows or not columns:
            return lead

        top_metric, top_metric_index = self._first_numeric_metric(columns, rows, metric_names)
        if top_metric_index is None:
            caveat = "Results are non-numeric, so no ranking insight was computed."
            return f"{lead} {caveat}"

        ranked = sorted(
            [row for row in rows if len(row) > top_metric_index and self._is_number(row[top_metric_index])],
            key=lambda r: float(r[top_metric_index]),
            reverse=True,
        )
        if not ranked:
            return f"{lead} Numeric metric '{top_metric}' was not populated in preview rows."

        best = ranked[0]
        dimension_label = self._best_dimension_label(columns, best, top_metric_index, dimension_names)
        best_metric_value = best[top_metric_index]
        caveat = "Preview may be truncated." if truncated else "Result preview represents full returned rows."

        return (
            f"{lead} Top insight: {dimension_label} leads on {top_metric} with {best_metric_value}. "
            f"{caveat}"
        )

    def _first_numeric_metric(
        self,
        columns: list[str],
        rows: list[list[Any]],
        metric_names: list[str],
    ) -> tuple[str, int | None]:
        metric_candidates = [m for m in metric_names if m in columns]
        for name in metric_candidates + columns:
            idx = columns.index(name)
            if any(len(row) > idx and self._is_number(row[idx]) for row in rows):
                return name, idx
        return "", None

    def _best_dimension_label(
        self,
        columns: list[str],
        row: list[Any],
        metric_index: int,
        dimension_names: list[str],
    ) -> str:
        dim_candidates = [d for d in dimension_names if d in columns]
        for dim in dim_candidates + columns:
            idx = columns.index(dim)
            if idx != metric_index and len(row) > idx and row[idx] is not None:
                return f"{dim}={row[idx]}"
        return "top row"

    def _is_metric_like(self, column: str) -> bool:
        label = column.lower()
        keywords = ("revenue", "sales", "amount", "count", "total", "sum", "avg", "mean", "units")
        return any(k in label for k in keywords)

    def _is_number(self, value: Any) -> bool:
        return isinstance(value, (int, float)) and not isinstance(value, bool)
