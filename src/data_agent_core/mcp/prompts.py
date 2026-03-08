from __future__ import annotations


def executive_summary_prompt(dataset_name: str, question: str) -> str:
    return (
        "Use analytics_run_analysis_plan with analysis_mode='executive_summary'. "
        f"Dataset: {dataset_name}. Question: {question}. "
        "Return top findings, confidence, caveats, and follow-up checks."
    )


def diagnostic_prompt(dataset_name: str, question: str) -> str:
    return (
        "Use analytics_run_analysis_plan with analysis_mode='diagnostic'. "
        f"Dataset: {dataset_name}. Question: {question}. "
        "Focus on anomalies, trend breaks, and data quality diagnostics."
    )


def deep_dive_prompt(dataset_name: str, question: str) -> str:
    return (
        "Use analytics_run_analysis_plan with analysis_mode='deep_dive'. "
        f"Dataset: {dataset_name}. Question: {question}. "
        "Run broader module coverage and prioritize highest-impact findings."
    )
