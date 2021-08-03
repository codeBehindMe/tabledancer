from typing import Any, Dict, List

from tabledancer.dancers.dancer import IDancer
from tabledancer.dancers.databricks.table_spec import DatabricksTableSpec
from tabledancer.models.table_spec import TableSpec
from pyspark.sql import SparkSession


class DatabricksDancer(IDancer):
    def __init__(
        self, workspace_id: str, token: str, cluster_id: str, port: str
    ) -> None:
    # FIXME: Docstring
        super().__init__()
        self.spark = SparkSession.builder.getOrCreate()

    def dance(self):
        # FIXME: Docstring
        pass

    def parse_table_spec(self, table_spec_dict: Dict[str, Any]) -> TableSpec:
        # FIXME: Docstring
        return DatabricksTableSpec(**table_spec_dict)

    # FIXME: Make abstract and enforce in IDancer interface.
    def _check_if_table_exists(self, table_spec : DatabricksTableSpec) -> bool:
        # FIXME: Docstring
        return self.spark._jsparkSession.catalog().tableExists(table_spec.database, table_spec.name)

    # FIXME: Make abstract and enforce in IDancer interface.
    def _get_table_ddl_in_backend(self, table_spec: DatabricksTableSpec) -> str:
        # FIXME: Docstring
        return self.spark.sql(f"SHOW CREATE TABLE {table_spec.database}.{table_spec.name}").collect()[0][0]

    def get_table_ddl_from_backend(self, table_spec: DatabricksTableSpec) -> str:
        # FIXME: Docstring

        if self._check_if_table_exists(table_spec) is not True:
            raise NotImplemented("Create table handler not implmented")

        ddl = self._get_table_ddl_in_backend(table_spec)
