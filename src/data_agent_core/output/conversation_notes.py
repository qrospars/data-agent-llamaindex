from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

from data_agent_core.output.models import ConversationTurn


class MarkdownConversationLogger:
    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def log_turn(self, session_id: str, turn: ConversationTurn) -> Path:
        path = self._session_path(session_id)
        is_new = not path.exists()

        with path.open("a", encoding="utf-8", newline="\n") as handle:
            if is_new:
                handle.write(f"# Conversation Notes: {session_id}\n\n")
                handle.write(
                    f"_Created: {datetime.now().isoformat(timespec='seconds')}_\n\n"
                )
            timestamp = datetime.now().isoformat(timespec="seconds")
            handle.write(f"## Turn ({timestamp})\n\n")
            handle.write(f"- Mode: `{turn.mode}`\n")
            handle.write(f"- User: {turn.user_message}\n\n")
            handle.write("### Assistant\n\n")
            handle.write(f"{turn.assistant_message}\n\n")
            if turn.sql:
                handle.write("### SQL\n\n")
                handle.write("```sql\n")
                handle.write(f"{turn.sql}\n")
                handle.write("```\n\n")
        return path

    def _session_path(self, session_id: str) -> Path:
        safe = re.sub(r"[^A-Za-z0-9._-]+", "_", session_id).strip("._")
        filename = safe or "default"
        return self.base_dir / f"{filename}.md"

