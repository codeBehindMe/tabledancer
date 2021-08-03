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

    def get_table_ddl_from_backend(self, table_spec: DatabricksTableSpec) -> str:
        pass