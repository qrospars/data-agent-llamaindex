from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class DatabaseConnector(ABC):
    @abstractmethod
    def connect(self) -> None:
        ...

    @abstractmethod
    def get_dialect(self) -> str:
        ...

    @abstractmethod
    def list_tables(self) -> list[str]:
        ...

    @abstractmethod
    def list_views(self) -> list[str]:
        ...

    @abstractmethod
    def get_columns(self, table_name: str) -> list[dict[str, Any]]:
        ...

    @abstractmethod
    def execute_readonly(self, sql: str, params: dict[str, Any] | None = None) -> tuple[list[str], list[list[Any]]]:
        ...
