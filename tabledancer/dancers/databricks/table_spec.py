from __future__ import annotations
from typing import Any, List

from tabledancer.dancers.dancer import IDancer
from tabledancer.models.table_spec import TableSpec


class DatabricksTableSpec(TableSpec):
    def __init__(self, name: str, columns: List[Any]) -> None:
        super().__init__(name, columns)

    def diff(other: DatabricksTableSpec) -> Any:
        return super().diff()
