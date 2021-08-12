from __future__ import annotations

from abc import ABCMeta, abstractmethod
from typing import Any, List


# FIXME [Aaron]: Mark as interface?
class TableSpec(metaclass=ABCMeta):
    def __init__(self, name: str, columns: List[Any]) -> None:
        # FIXME: Docstring
        self.name = name
        self.columns = columns

    @abstractmethod
    def diff(self, other: TableSpec) -> Any:
        # FIXME: Docstring
        pass  # FIXME: Raise NotImplementedError

    @abstractmethod
    def is_diff(self, other: TableSpec) -> bool:
        # FIXME: Docstring
        raise NotImplementedError(f"Not implementeded method {self.is_diff.__name__}")
