from __future__ import annotations

import re
from dataclasses import dataclass, field

MUTATING_KEYWORDS = {
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "truncate",
    "grant",
    "revoke",
    "create",
    "replace",
}
FORBIDDEN_SYSTEM_PATTERNS = ("sqlite_master", "information_schema", "pg_catalog")


@dataclass
class ValidationResult:
    passed: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    normalized_sql: str = ""


class SQLValidator:
    def __init__(
        self,
        forbidden_tables: list[str] | None = None,
        allowed_tables: list[str] | None = None,
        max_row_limit: int = 1000,
    ) -> None:
        self.forbidden_tables = [item.lower() for item in (forbidden_tables or [])]
        self.allowed_tables = [item.lower() for item in (allowed_tables or [])]
        self.max_row_limit = max_row_limit

    def _strip_comments(self, sql: str) -> str:
        sql = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
        sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
        return sql

    def _has_multiple_statements(self, sql: str) -> bool:
        # Ignore a trailing semicolon.
        stripped = sql.strip()
        if stripped.endswith(";"):
            stripped = stripped[:-1].rstrip()
        return ";" in stripped

    def _extract_table_refs(self, lowered_sql: str) -> set[str]:
        matches = re.findall(r"\b(?:from|join)\s+([a-zA-Z_][\w\.]*)", lowered_sql)
        return {m.split(".")[-1] for m in matches}

    def validate(self, sql: str) -> ValidationResult:
        normalized = self._strip_comments(sql).strip()
        lowered = normalized.lower()
        result = ValidationResult(passed=True, normalized_sql=normalized)

        if not normalized:
            result.passed = False
            result.errors.append("SQL is empty")
            return result

        if self._has_multiple_statements(normalized):
            result.passed = False
            result.errors.append("Multiple statements are not allowed")

        if not lowered.startswith(("select", "with")):
            result.passed = False
            result.errors.append("Only SELECT/CTE queries are allowed")

        for keyword in MUTATING_KEYWORDS:
            if re.search(rf"\b{re.escape(keyword)}\b", lowered):
                result.passed = False
                result.errors.append(f"Forbidden keyword detected: {keyword}")

        for sys_pattern in FORBIDDEN_SYSTEM_PATTERNS:
            if re.search(rf"\b{re.escape(sys_pattern)}\b", lowered):
                result.passed = False
                result.errors.append(f"System table/schema access blocked: {sys_pattern}")

        table_refs = self._extract_table_refs(lowered)

        for table in self.forbidden_tables:
            if table in table_refs:
                result.passed = False
                result.errors.append(f"Forbidden table referenced: {table}")

        if self.allowed_tables:
            disallowed = sorted(table_refs.difference(self.allowed_tables))
            if disallowed:
                result.passed = False
                result.errors.append(
                    f"Disallowed table(s) referenced: {', '.join(disallowed)}. "
                    "Use only allowed_tables."
                )

        return result
