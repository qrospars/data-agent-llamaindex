from __future__ import annotations

from typing import Any

from data_agent_core.config.models import ProviderConfig
from data_agent_core.llm.gemini_provider import create_gemini_llm


class MockLLM:
    def complete(self, prompt: str) -> str:
        _ = prompt
        return "SELECT 1 AS ok"


def create_llm(config: ProviderConfig) -> Any:
    if config.provider == "gemini":
        return create_gemini_llm(config)
    return MockLLM()
