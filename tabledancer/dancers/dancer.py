from abc import ABCMeta, abstractmethod
from typing import Any, Dict

from tabledancer.models.table_spec import TableSpec


class IDancer(metaclass=ABCMeta):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def dance(self):
        pass

    @abstractmethod
    def parse_table_spec(self, table_spec_dict: Dict[str, Any]) -> TableSpec:
        pass