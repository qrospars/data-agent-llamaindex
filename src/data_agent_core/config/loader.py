from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from data_agent_core.config.models import AppConfig, SemanticConfig


def load_semantic_config(path: Path | None = None, data: dict[str, Any] | None = None) -> SemanticConfig:
    if data is not None:
        return SemanticConfig.model_validate(data)
    if path is None:
        return SemanticConfig()

    payload: dict[str, Any]
    text = path.read_text()
    if path.suffix.lower() in {".yaml", ".yml"}:
        try:
            import yaml
        except ModuleNotFoundError as exc:
            raise RuntimeError("PyYAML is required to load YAML semantic config files") from exc
        payload = yaml.safe_load(text) or {}
    elif path.suffix.lower() == ".json":
        payload = json.loads(text)
    else:
        raise ValueError("Semantic config must be YAML or JSON")
    return SemanticConfig.model_validate(payload)


def build_config(**kwargs: Any) -> AppConfig:
    config = AppConfig.model_validate(kwargs)
    if config.semantic_config_data is None and config.semantic_config_path:
        config.semantic_config_data = load_semantic_config(path=config.semantic_config_path).model_dump()
    return config
