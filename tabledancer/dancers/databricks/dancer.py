from typing import Any, Dict, List

from pyspark.sql import SparkSession

from tabledancer.dancers.dancer import IDancer
from tabledancer.dancers.databricks.parser import DatabricksDDLParser
from tabledancer.dancers.databricks.table_spec import DatabricksTableSpec
from tabledancer.models.lifecycle_policy import (DropCreateOnSchemaChange,
                                                 ErrorOnSchemaChange,
                                                 EvolveOnSchemaChange,
                                                 LifeCyclePolicy,
                                                 life_cycle_policies)
from tabledancer.models.lifecycle_spec import LifeCycleSpec
from tabledancer.models.table_spec import TableSpec


class DatabricksDancer(IDancer):
    def __init__(
        self, workspace_id: str, token: str, cluster_id: str, port: str
    ) -> None:
        # FIXME: Docstring
        super().__init__()
        self.spark = SparkSession.builder.getOrCreate()

    def dance(self, life_cycle_spec_dict: Dict[str, Any]):
        # FIXME: Docstring
        parsed_spec = self._parse_life_cycle_spec_dict(life_cycle_spec_dict)
        table_spec: DatabricksTableSpec = parsed_spec.table_spec
        if not self._check_if_table_exists(table_spec):
            raise NotImplementedError("Action not implemented")

        if not self._check_if_table_exists(table_spec):
            raise NotImplementedError("Implement this")  # FIXME: Implement this this

        existing_ddl = self._get_table_ddl_in_backend(table_spec)
        existing_table_spec = DatabricksDDLParser(existing_ddl).to_table_spec(
            existing_ddl
        )

        if table_spec.is_diff(existing_table_spec):
            raise NotImplementedError("Implement this")
        # FIXME: Implement actions

    def take_life_cycle_action(
        self, life_cycle_policy: LifeCyclePolicy, target_spec: TableSpec
    ):
        # FIXME: Docstring

        if isinstance(life_cycle_policy, DropCreateOnSchemaChange):
            raise NotImplementedError("Implement this")
        elif isinstance(life_cycle_policy, ErrorOnSchemaChange):
            raise NotImplementedError("Implement this")
        elif isinstance(life_cycle_policy, EvolveOnSchemaChange):
            raise NotImplementedError("Implement this")
        else:
            raise NotImplementedError("Unsupported life cycle policy")

    def _parse_life_cycle_spec_dict(self, life_cycle_spec_dict: Dict[str, Any]):
        # FIXME: Docstring

        life_cycle_spec_dict["life_cycle_policy"] = self._parse_life_cycle_policy_dict(
            life_cycle_spec_dict["life_cycle_policy"]
        )

        life_cycle_spec_dict["table_spec"] = self.parse_table_spec(
            life_cycle_spec_dict["table_spec"]
        )
        return LifeCycleSpec(**life_cycle_spec_dict)

    def _parse_life_cycle_policy_dict(
        self, life_cycle_policy_dict: Dict[str, Any]
    ) -> LifeCyclePolicy:
        # FIXME: Docstring

        # FIXME: Strings in code which are reused in YAML files and possibly
        #   other locations in the future.
        policy_name = life_cycle_policy_dict["policy"]
        policy_properties = life_cycle_policy_dict["properties"]

        if policy_properties is not None:
            return life_cycle_policies[policy_name](**policy_properties)
        return life_cycle_policies[policy_name]

    # FIXME: Change this method to parse_table_spec_dict
    def parse_table_spec(self, table_spec_dict: Dict[str, Any]) -> TableSpec:
        # FIXME: Docstring
        return DatabricksTableSpec(**table_spec_dict)

    # FIXME: Make abstract and enforce in IDancer interface.
    def _check_if_table_exists(self, table_spec: DatabricksTableSpec) -> bool:
        # FIXME: Docstring
        return self.spark._jsparkSession.catalog().tableExists(
            table_spec.database, table_spec.name
        )

    # FIXME: Make abstract and enforce in IDancer interface.
    def _get_table_ddl_in_backend(self, table_spec: DatabricksTableSpec) -> str:
        # FIXME: Docstring
        return self.spark.sql(
            f"SHOW CREATE TABLE {table_spec.database}.{table_spec.name}"
        ).collect()[0][0]

    def get_table_ddl_from_backend(self, table_spec: DatabricksTableSpec) -> str:
        # FIXME: Docstring

        if self._check_if_table_exists(table_spec) is not True:
            raise NotImplementedError("Create table handler not implmented")

        ddl = self._get_table_ddl_in_backend(table_spec)
        return DatabricksDDLParser(ddl).to_table_spec(ddl)
