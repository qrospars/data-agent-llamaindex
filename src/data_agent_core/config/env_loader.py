from __future__ import annotations

import os
from pathlib import Path


def load_env_file(path: Path | None = None, override: bool = False) -> Path | None:
    candidate = path or Path.cwd() / ".env"
    if not candidate.exists():
        return None

    for raw_line in candidate.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.lower().startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        env_key = key.strip()
        env_value = _normalize_value(value.strip())
        if not env_key:
            continue
        if override or env_key not in os.environ:
            os.environ[env_key] = env_value

    return candidate


def _normalize_value(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value

