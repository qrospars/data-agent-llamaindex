from __future__ import annotations

import os
from pathlib import Path

from data_agent_core.config.env_loader import load_env_file


def test_load_env_file_sets_values(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text("FOO=bar\nQUOTED='hello world'\n")

    load_env_file(path=env_path)

    assert os.getenv("FOO") == "bar"
    assert os.getenv("QUOTED") == "hello world"


def test_load_env_file_respects_existing_env(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text("EXISTING=new\n")
    os.environ["EXISTING"] = "old"

    load_env_file(path=env_path, override=False)
    assert os.getenv("EXISTING") == "old"

    load_env_file(path=env_path, override=True)
    assert os.getenv("EXISTING") == "new"
