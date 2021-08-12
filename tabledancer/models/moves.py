from abc import ABC, abstractmethod

from tabledancer.models.lifecycle_policy import (DropCreateOnSchemaChange,
                                                 ErrorOnSchemaChange,
                                                 EvolveOnSchemaChange)
from tabledancer.models.table_spec import TableSpec


class IMoves(ABC):
    @abstractmethod
    def table_does_not_exist_move(self, table_spec: TableSpec):
        # FIXME: Docstring
        pass

    @abstractmethod
    def drop_create_on_schema_change_move(
        self, table_spec: TableSpec, drop_create_policy: DropCreateOnSchemaChange
    ):
        # FIXME: Docstring
        pass

    @abstractmethod
    def error_on_schema_change_move(self, raise_error_policy: ErrorOnSchemaChange):
        # FIXME: Docstring
        pass

    @abstractmethod
    def evolve_on_schema_change_move(
        self, target_table_spec: TableSpec, evolve_table_policy: EvolveOnSchemaChange
    ):
        # FIXME: Docstring
        pass
