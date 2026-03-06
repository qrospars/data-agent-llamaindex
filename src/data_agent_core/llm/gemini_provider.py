from __future__ import annotations

import os
from typing import Any

from data_agent_core.config.models import ProviderConfig


def create_gemini_llm(config: ProviderConfig) -> Any:
    from llama_index.llms.google_genai import GoogleGenAI

    api_key = os.getenv(config.api_key_env)
    if not api_key:
        raise ValueError(f"Environment variable {config.api_key_env} is not set")
    return GoogleGenAI(model=config.model, api_key=api_key, temperature=config.temperature)
