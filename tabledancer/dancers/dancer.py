from abc import ABCMeta, abstractmethod
from typing import Any, Dict

from tabledancer.models.lifecycle_policy import LifeCyclePolicy
from tabledancer.models.table_spec import TableSpec


class IDancer(metaclass=ABCMeta):
    def __init__(self) -> None:
        # FIXME: Docstring
        pass

    @abstractmethod
    def dance(self):
        # FIXME: Docstring
        pass

    @abstractmethod
    def parse_table_spec(self, table_spec_dict: Dict[str, Any]) -> TableSpec:
        # FIXME: Docstring
        pass

    @abstractmethod
    def get_table_ddl_from_backend(self, table_spec: TableSpec) -> str:
        # FIXME: Docstring
        pass

    @abstractmethod
    def take_life_cycle_action(
        self, life_cycle_policy: LifeCyclePolicy, target_spec: TableSpec
    ):
        # FIXME: Docstring
        pass
