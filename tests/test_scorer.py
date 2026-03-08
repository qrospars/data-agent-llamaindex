from __future__ import annotations

from data_agent_core.analytics.models import EvidenceRef, Finding
from data_agent_core.analytics.scorer import FindingScorer


def test_scorer_ranks_by_weighted_score() -> None:
    scorer = FindingScorer()
    findings = [
        Finding(
            title="Low",
            observation="small",
            impact_score=0.2,
            confidence_score=0.9,
            business_relevance_score=0.4,
            severity="low",
            evidence=[EvidenceRef(module="x", query_id="1", table_name="t")],
        ),
        Finding(
            title="High",
            observation="large",
            impact_score=0.9,
            confidence_score=0.7,
            business_relevance_score=0.8,
            severity="low",
            evidence=[EvidenceRef(module="x", query_id="2", table_name="t")],
        ),
    ]

    ranked = scorer.rank_findings(findings)
    assert ranked[0].title == "High"
    assert ranked[0].severity in {"medium", "high"}
