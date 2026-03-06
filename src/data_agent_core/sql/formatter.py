from __future__ import annotations

import re

LIMIT_PATTERN = re.compile(r"\blimit\s+(\d+)\b", flags=re.IGNORECASE)


def extract_limit(sql: str) -> int | None:
    match = LIMIT_PATTERN.search(sql)
    if not match:
        return None
    return int(match.group(1))


def ensure_limit(sql: str, row_limit: int, max_row_limit: int) -> tuple[str, bool, list[str]]:
    """Ensure SQL has a LIMIT and cap it by max_row_limit.

    Returns: (sql, rewritten, warnings)
    """
    warnings: list[str] = []
    safe_limit = min(max(1, row_limit), max_row_limit)
    existing_limit = extract_limit(sql)

    if existing_limit is None:
        return f"{sql.rstrip(';')} LIMIT {safe_limit}", True, warnings

    if existing_limit > max_row_limit:
        rewritten = LIMIT_PATTERN.sub(f"LIMIT {max_row_limit}", sql, count=1)
        warnings.append(f"LIMIT {existing_limit} capped to max_row_limit={max_row_limit}")
        return rewritten, True, warnings

    return sql, False, warnings
