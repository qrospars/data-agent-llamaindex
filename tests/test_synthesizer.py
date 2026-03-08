from __future__ import annotations

from data_agent_core.analytics.models import EvidenceBundle, EvidenceRef, Finding
from data_agent_core.analytics.synthesizer import SynthesisService


def test_synthesizer_builds_summary_with_caveats() -> None:
    service = SynthesisService()
    bundle = EvidenceBundle(
        dataset_name="sales",
        question="What changed?",
        findings=[
            Finding(
                title="Revenue drop in North",
                observation="North revenue decreased by 12%",
                impact_score=0.7,
                confidence_score=0.8,
                business_relevance_score=0.85,
                severity="high",
                caveats=["Possible seasonality"],
                evidence=[EvidenceRef(module="trends", query_id="q1", table_name="sales")],
            )
        ],
        raw_module_outputs={},
    )

    summary = service.build_executive_summary(bundle)
    assert summary.headline
    assert summary.top_findings
    assert summary.caveats
    assert summary.follow_up_questions
