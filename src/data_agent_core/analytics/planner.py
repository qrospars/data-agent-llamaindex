from __future__ import annotations

import json
from pathlib import Path

from data_agent_core.analytics.models import AnalysisPlan, AnalysisRequest, AnalysisTask, DatasetProfile, SemanticModel
from data_agent_core.config.models import ProviderConfig
from data_agent_core.llm.factory import create_llm


class AnalysisPlanner:
    def __init__(self, max_tasks: int = 8, llm_provider: ProviderConfig | None = None) -> None:
        self.max_tasks = max_tasks
        self.provider = llm_provider or ProviderConfig(provider="mock")
        self.llm = create_llm(self.provider)

    def create_plan(
        self,
        request: AnalysisRequest,
        profile: DatasetProfile,
        semantic_model: SemanticModel,
    ) -> AnalysisPlan:
        deterministic = self._build_deterministic_plan(request, semantic_model)
        if self.provider.provider == "mock":
            return deterministic

        try:
            template = self._prompt_template("planner.xml")
            prompt = template.format(
                dataset_name=request.dataset_name,
                question=request.question,
                analysis_mode=request.analysis_mode,
                profile_json=profile.model_dump_json(indent=2),
                semantic_json=semantic_model.model_dump_json(indent=2),
            )
            completion = self.llm.complete(prompt)
            raw = completion.text if hasattr(completion, "text") else str(completion)
            payload = self._extract_json(raw)
            candidate = AnalysisPlan.model_validate(payload)
            candidate.tasks = self._validate_tasks(candidate.tasks, semantic_model)
            candidate.tasks = sorted(candidate.tasks, key=lambda task: task.priority)[: self.max_tasks]
            return candidate
        except Exception:
            return deterministic

    def _build_deterministic_plan(
        self,
        request: AnalysisRequest,
        semantic_model: SemanticModel,
    ) -> AnalysisPlan:
        metric_names = [metric.name for metric in semantic_model.metrics]
        dimension_names = [dim.name for dim in semantic_model.dimensions]

        tasks: list[AnalysisTask] = []
        if request.analysis_mode == "overview":
            tasks.extend(
                [
                    AnalysisTask(module="overview", priority=1),
                    AnalysisTask(module="diagnostics", priority=2),
                ]
            )
        elif request.analysis_mode == "executive_summary":
            tasks.append(AnalysisTask(module="overview", priority=1))
            for index, metric in enumerate(metric_names[:2], start=2):
                tasks.append(AnalysisTask(module="trends", metric=metric, grain="month", priority=index))
            start = len(tasks) + 1
            for index, dimension in enumerate(dimension_names[:2], start=start):
                if not metric_names:
                    break
                tasks.append(
                    AnalysisTask(
                        module="segments",
                        metric=metric_names[0],
                        dimension=dimension,
                        priority=index,
                    )
                )
            tasks.append(AnalysisTask(module="diagnostics", priority=len(tasks) + 1))
        elif request.analysis_mode == "diagnostic":
            for index, metric in enumerate(metric_names[:2], start=1):
                tasks.append(AnalysisTask(module="trends", metric=metric, grain="week", priority=index))
                tasks.append(AnalysisTask(module="anomalies", metric=metric, grain="week", priority=index + 10))
            tasks.append(AnalysisTask(module="diagnostics", priority=len(tasks) + 1))
        elif request.analysis_mode == "deep_dive":
            tasks.append(AnalysisTask(module="overview", priority=1))
            priority = 2
            for metric in metric_names[:3]:
                tasks.append(AnalysisTask(module="trends", metric=metric, grain="month", priority=priority))
                priority += 1
                tasks.append(AnalysisTask(module="anomalies", metric=metric, grain="week", priority=priority))
                priority += 1
            if metric_names:
                for dimension in dimension_names[:3]:
                    tasks.append(
                        AnalysisTask(
                            module="segments",
                            metric=metric_names[0],
                            dimension=dimension,
                            priority=priority,
                        )
                    )
                    priority += 1
            tasks.append(AnalysisTask(module="diagnostics", priority=priority))
        else:
            raise ValueError(f"Unsupported analysis mode: {request.analysis_mode}")

        tasks = self._validate_tasks(tasks, semantic_model)
        tasks = sorted(tasks, key=lambda task: task.priority)[: self.max_tasks]

        return AnalysisPlan(
            dataset_name=request.dataset_name,
            goal=request.question,
            tasks=tasks,
            reasoning_summary=(
                "Plan built from analysis mode and validated against semantic model constraints."
            ),
        )

    def _validate_tasks(self, tasks: list[AnalysisTask], semantic_model: SemanticModel) -> list[AnalysisTask]:
        metric_names = {metric.name for metric in semantic_model.metrics}
        dimension_names = {dim.name for dim in semantic_model.dimensions}

        for task in tasks:
            if task.metric and task.metric not in metric_names:
                raise ValueError(f"Unknown metric in task: {task.metric}")
            if task.dimension and task.dimension not in dimension_names:
                raise ValueError(f"Unknown dimension in task: {task.dimension}")
        return tasks

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
