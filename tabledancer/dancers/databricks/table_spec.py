from __future__ import annotations

from typing import Any, List, Optional

from tabledancer.dancers.dancer import IDancer
from tabledancer.models.table_spec import TableSpec


class DatabricksTableSpec(TableSpec):
    def __init__(
        self,
        name: str,
        columns: List[Any],
        comment: Optional[str] = None,
        using: Optional[str] = None,
        partitions: Optional[List[str]] = None,
        location: Optional[str] = None,
        options: Optional[List[str]] = None,
    ) -> None:
        super().__init__(name, columns)
        self.comment = comment
        self.using = using
        self.partitions = partitions
        self.location = location
        self.options = options

    def diff(other: DatabricksTableSpec) -> Any:
        return super().diff()
