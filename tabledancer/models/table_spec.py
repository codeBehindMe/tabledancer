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
    def diff(other: TableSpec) -> Any:
        # FIXME: Docstring
        pass
