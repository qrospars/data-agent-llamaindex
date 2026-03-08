from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml
from llama_index.core.workflow import Event, StartEvent, StopEvent, Workflow, step

from data_agent_core.analytics.models import (
    AnalysisPlan,
    AnalysisRequest,
    DatasetProfile,
    EvidenceBundle,
    RunAnalysisInput,
    RunAnalysisOutput,
    SemanticModel,
    TableSchema,
)
from data_agent_core.analytics.modules import (
    AnomaliesModule,
    DiagnosticsModule,
    OverviewModule,
    SegmentsModule,
    TrendsModule,
)
from data_agent_core.analytics.modules._base import ModuleContext
from data_agent_core.analytics.planner import AnalysisPlanner
from data_agent_core.analytics.profiler import DatasetProfiler
from data_agent_core.analytics.scorer import FindingScorer
from data_agent_core.analytics.semantic_inference import SemanticInferenceService
from data_agent_core.analytics.synthesizer import SynthesisService


class LoadedContextEvent(Event):
    request: AnalysisRequest
    semantic_model: SemanticModel


class PlannedEvent(Event):
    request: AnalysisRequest
    semantic_model: SemanticModel
    plan: AnalysisPlan


class ExecutedEvent(Event):
    request: AnalysisRequest
    plan: AnalysisPlan
    evidence_bundle: EvidenceBundle


class ScoredEvent(Event):
    request: AnalysisRequest
    plan: AnalysisPlan
    evidence_bundle: EvidenceBundle


class RunAnalysisWorkflow(Workflow):
    def __init__(
        self,
        state_root: str | Path = ".state",
        db_path: str | Path = ".state/analytics.duckdb",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.context = ModuleContext(db_path=db_path, state_root=state_root)
        self.profiler = DatasetProfiler(db_path=db_path, state_root=state_root)
        self.semantic_service = SemanticInferenceService(state_root=state_root)
        self.planner = AnalysisPlanner(max_tasks=8)
        self.scorer = FindingScorer()
        self.synthesizer = SynthesisService()

        self.overview_module = OverviewModule(context=self.context)
        self.trends_module = TrendsModule(context=self.context)
        self.segments_module = SegmentsModule(context=self.context)
        self.anomalies_module = AnomaliesModule(context=self.context)
        self.diagnostics_module = DiagnosticsModule(context=self.context)

    @step
    async def load_context(self, ev: StartEvent) -> LoadedContextEvent:
        payload = RunAnalysisInput(
            dataset_name=str(ev.get("dataset_name")),
            question=str(ev.get("question")),
            analysis_mode=str(ev.get("analysis_mode")),
        )

        dataset = self.context.registry.get_dataset(payload.dataset_name)
        profile = self.profiler.profile_dataset(payload.dataset_name, dataset.table_name)

        if dataset.semantic_path and Path(dataset.semantic_path).exists():
            semantic_text = Path(dataset.semantic_path).read_text(encoding="utf-8")
            if Path(dataset.semantic_path).suffix.lower() in {".yaml", ".yml"}:
                semantic_payload = yaml.safe_load(semantic_text) or {}
            else:
                semantic_payload = json.loads(semantic_text)
            semantic_model = SemanticModel.model_validate(semantic_payload)
        else:
            schema = TableSchema(
                table_name=dataset.table_name,
                columns=self.context.loader.get_schema(dataset.table_name),
            )
            semantic_model = self.semantic_service.build_semantic_model(
                profile=profile,
                schema=schema,
            )

        request = AnalysisRequest(
            dataset_name=payload.dataset_name,
            question=payload.question,
            analysis_mode=payload.analysis_mode,
        )
        return LoadedContextEvent(request=request, semantic_model=semantic_model)

    @step
    async def plan(self, ev: LoadedContextEvent) -> PlannedEvent:
        dataset = self.context.registry.get_dataset(ev.request.dataset_name)
        profile_path = self.context.registry.profiles_dir / f"{ev.request.dataset_name}.json"
        if profile_path.exists():
            profile = DatasetProfile.model_validate_json(profile_path.read_text(encoding="utf-8"))
        else:
            profile = self.profiler.profile_dataset(ev.request.dataset_name, dataset.table_name)

        plan = self.planner.create_plan(
            request=ev.request,
            profile=profile,
            semantic_model=ev.semantic_model,
        )
        return PlannedEvent(request=ev.request, semantic_model=ev.semantic_model, plan=plan)

    @step
    async def execute_tasks(self, ev: PlannedEvent) -> ExecutedEvent:
        all_findings = []
        raw_outputs: dict[str, Any] = {}

        for task in ev.plan.tasks:
            if task.module == "overview":
                result = self.overview_module.run(ev.request.dataset_name, ev.semantic_model)
            elif task.module == "trends":
                if not task.metric:
                    raise ValueError("Trends task requires metric")
                result = self.trends_module.run(
                    ev.request.dataset_name,
                    metric=task.metric,
                    grain=task.grain or "month",
                )
            elif task.module == "segments":
                if not task.metric or not task.dimension:
                    raise ValueError("Segments task requires metric and dimension")
                result = self.segments_module.run(
                    ev.request.dataset_name,
                    metric=task.metric,
                    dimension=task.dimension,
                )
            elif task.module == "anomalies":
                if not task.metric:
                    raise ValueError("Anomalies task requires metric")
                result = self.anomalies_module.run(
                    ev.request.dataset_name,
                    metric=task.metric,
                    grain=task.grain or "week",
                )
            elif task.module == "diagnostics":
                result = self.diagnostics_module.run(ev.request.dataset_name)
            else:
                raise ValueError(f"Unsupported module: {task.module}")

            raw_outputs[f"{task.module}:{task.priority}"] = result.get("raw_module_outputs", {})
            all_findings.extend(result.get("findings", []))

        bundle = EvidenceBundle(
            dataset_name=ev.request.dataset_name,
            question=ev.request.question,
            findings=all_findings,
            raw_module_outputs=raw_outputs,
        )
        evidence_path = self.context.registry.evidence_dir / f"{ev.request.dataset_name}_latest.json"
        evidence_path.write_text(bundle.model_dump_json(indent=2), encoding="utf-8")

        return ExecutedEvent(request=ev.request, plan=ev.plan, evidence_bundle=bundle)

    @step
    async def score(self, ev: ExecutedEvent) -> ScoredEvent:
        ranked = self.scorer.rank_findings(ev.evidence_bundle.findings)
        bundle = ev.evidence_bundle.model_copy(update={"findings": ranked})
        return ScoredEvent(request=ev.request, plan=ev.plan, evidence_bundle=bundle)

    @step
    async def synthesize(self, ev: ScoredEvent) -> StopEvent:
        summary = self.synthesizer.build_executive_summary(ev.evidence_bundle)
        output = RunAnalysisOutput(
            plan=ev.plan,
            evidence_bundle=ev.evidence_bundle,
            executive_summary=summary,
        )
        return StopEvent(result=output)

    async def run_analysis(self, payload: RunAnalysisInput) -> RunAnalysisOutput:
        handler = self.run(
            dataset_name=payload.dataset_name,
            question=payload.question,
            analysis_mode=payload.analysis_mode,
        )
        result = await handler
        if isinstance(result, RunAnalysisOutput):
            return result
        if isinstance(result, dict):
            return RunAnalysisOutput.model_validate(result)
        return RunAnalysisOutput.model_validate(result.model_dump())
