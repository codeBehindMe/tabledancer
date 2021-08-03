from typing import Any, Dict, List

from tabledancer.dancers.dancer import IDancer
from tabledancer.dancers.databricks.table_spec import DatabricksTableSpec
from tabledancer.models.table_spec import TableSpec


class DatabricksDancer(IDancer):
    def __init__(
        self, workspace_id: str, token: str, cluster_id: str, port: str
    ) -> None:
    # FIXME: Docstring
        super().__init__()

    def dance(self):
        # FIXME: Docstring
        pass

    def parse_table_spec(self, table_spec_dict: Dict[str, Any]) -> TableSpec:
        # FIXME: Docstring
        return DatabricksTableSpec(**table_spec_dict)
