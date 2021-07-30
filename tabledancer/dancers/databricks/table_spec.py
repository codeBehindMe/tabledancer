from __future__ import annotations

from typing import Any, List, Optional

from tabledancer.dancers.dancer import IDancer
from tabledancer.models.table_spec import TableSpec
from tabledancer.utils.misc import is_none_or_empty_string


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
        super().__init__(self._validate_name(name), columns)
        self.comment = comment
        self.using = using
        self.partitions = self._validate_partitions(partitions)
        self.location = self._validate_location(location)
        self.options = self._validate_options(options)

    @staticmethod
    def _validate_name(name: str) -> str:

        if is_none_or_empty_string(name):
            raise ValueError("Name cannot be empty or None")
        return name

    @staticmethod
    def _validate_partitions(partitions: Optional[List[str]]) -> Optional[List[str]]:
        if partitions is not None:
            raise NotImplementedError("Partitions not supported")
        return partitions

    @staticmethod
    def _validate_location(location: Optional[str]) -> Optional[str]:
        if location is not None:
            raise NotImplementedError("Location not supported")
        return location

    @staticmethod
    def _validate_options(options: Optional[List[str]]) -> Optional[List[str]]:
        if options is not None:
            raise NotImplementedError("Options not supported")
        return options

    def diff(other: DatabricksTableSpec) -> Any:
        return super().diff()
