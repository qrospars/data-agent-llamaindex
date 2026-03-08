from __future__ import annotations

from data_agent_core.analytics.models import RunAnalysisInput
from data_agent_core.analytics.workflow import RunAnalysisWorkflow


async def analytics_run_analysis_plan(
    dataset_name: str,
    question: str,
    analysis_mode: str,
    state_root: str = ".state",
    db_path: str = ".state/analytics.duckdb",
) -> dict[str, object]:
    workflow = RunAnalysisWorkflow(state_root=state_root, db_path=db_path, timeout=180)
    payload = RunAnalysisInput(dataset_name=dataset_name, question=question, analysis_mode=analysis_mode)
    output = await workflow.run_analysis(payload)
    return {
        "plan": output.plan.model_dump(mode="json"),
        "top_findings": [finding.model_dump(mode="json") for finding in output.evidence_bundle.findings[:3]],
        "executive_summary": output.executive_summary.model_dump(mode="json"),
        "caveats": output.executive_summary.caveats,
    }
