from __future__ import annotations

import re

from data_agent_core.analytics.models import Finding


class FindingScorer:
    def __init__(
        self,
        impact_weight: float = 0.45,
        confidence_weight: float = 0.35,
        business_relevance_weight: float = 0.20,
    ) -> None:
        self.impact_weight = impact_weight
        self.confidence_weight = confidence_weight
        self.business_relevance_weight = business_relevance_weight

    def score_finding(self, finding: Finding) -> Finding:
        impact = self._clamp(self._derive_impact(finding))
        confidence = self._clamp(self._derive_confidence(finding))
        business = self._clamp(self._derive_business_relevance(finding))
        combined = (
            impact * self.impact_weight
            + confidence * self.confidence_weight
            + business * self.business_relevance_weight
        )
        severity = "high" if combined >= 0.7 else "medium" if combined >= 0.4 else "low"

        return finding.model_copy(
            update={
                "impact_score": impact,
                "confidence_score": confidence,
                "business_relevance_score": business,
                "severity": severity,
            }
        )

    def rank_findings(self, findings: list[Finding]) -> list[Finding]:
        scored = [self.score_finding(item) for item in findings]
        return sorted(
            scored,
            key=lambda item: (
                item.impact_score * self.impact_weight
                + item.confidence_score * self.confidence_weight
                + item.business_relevance_score * self.business_relevance_weight
            ),
            reverse=True,
        )

    def _derive_impact(self, finding: Finding) -> float:
        if finding.impact_score > 0:
            return finding.impact_score
        numbers = [float(value) for value in re.findall(r"-?\d+(?:\.\d+)?", finding.observation)]
        if not numbers:
            return 0.3
        peak = max(abs(value) for value in numbers)
        return min(1.0, peak / 100.0)

    def _derive_confidence(self, finding: Finding) -> float:
        if finding.confidence_score > 0:
            return finding.confidence_score
        base = 0.75 if finding.evidence else 0.4
        if finding.caveats:
            base -= min(0.3, 0.08 * len(finding.caveats))
        return max(0.1, base)

    def _derive_business_relevance(self, finding: Finding) -> float:
        if finding.business_relevance_score > 0:
            return finding.business_relevance_score
        base = 0.5
        if finding.metric:
            base += 0.2
        if "risk" in finding.title.lower() or "risk" in finding.observation.lower():
            base += 0.15
        if "revenue" in finding.observation.lower() or "sales" in finding.observation.lower():
            base += 0.1
        return min(1.0, base)

    def _clamp(self, value: float) -> float:
        return max(0.0, min(1.0, float(value)))
