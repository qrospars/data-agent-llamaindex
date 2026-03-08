from __future__ import annotations

import json
from pathlib import Path

from data_agent_core.analytics.models import EvidenceBundle, ExecutiveSummary
from data_agent_core.config.models import ProviderConfig
from data_agent_core.llm.factory import create_llm


class SynthesisService:
    def __init__(self, llm_provider: ProviderConfig | None = None) -> None:
        self.provider = llm_provider or ProviderConfig(provider="mock")
        self.llm = create_llm(self.provider)

    def build_executive_summary(self, bundle: EvidenceBundle) -> ExecutiveSummary:
        if self.provider.provider == "mock":
            return self._deterministic_summary(bundle)

        ranked_findings = [
            {
                "title": finding.title,
                "observation": finding.observation,
                "interpretation": finding.interpretation,
                "severity": finding.severity,
                "caveats": finding.caveats,
            }
            for finding in bundle.findings
        ]
        template = self._prompt_template("synthesis.xml")
        prompt = template.format(
            dataset_name=bundle.dataset_name,
            question=bundle.question,
            findings_json=json.dumps(ranked_findings, indent=2),
        )

        completion = self.llm.complete(prompt)
        raw = completion.text if hasattr(completion, "text") else str(completion)
        payload = self._extract_json(raw)
        summary = ExecutiveSummary.model_validate(payload)

        known_titles = {finding.title for finding in bundle.findings}
        if any(title not in known_titles for title in summary.top_findings):
            raise ValueError("Synthesis output referenced unknown findings")

        return summary

    def _deterministic_summary(self, bundle: EvidenceBundle) -> ExecutiveSummary:
        findings = bundle.findings[:3]
        headline = findings[0].title if findings else "No material findings"
        top_findings = [finding.title for finding in findings]
        key_risks = [finding.observation for finding in bundle.findings if finding.severity == "high"][:3]
        caveats: list[str] = []
        for finding in findings:
            caveats.extend(finding.caveats)
        if not caveats:
            caveats.append("Summary is constrained to deterministic evidence only.")

        follow_up = [
            f"Validate drivers behind: {finding.title}" for finding in findings[:2]
        ]
        if not follow_up:
            follow_up.append("Collect more data to produce actionable findings.")

        return ExecutiveSummary(
            headline=headline,
            top_findings=top_findings,
            key_risks=key_risks,
            caveats=caveats[:5],
            follow_up_questions=follow_up,
        )

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
