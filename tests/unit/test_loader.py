from pathlib import Path

import pytest

pytest.importorskip("pydantic")
pytest.importorskip("yaml")

from data_agent_core.config.loader import load_semantic_config


def test_load_yaml(tmp_path: Path) -> None:
    path = tmp_path / "semantic.yaml"
    path.write_text("project: demo\n")
    semantic = load_semantic_config(path=path)
    assert semantic.project == "demo"
