from typing import Any, Dict, List

from tabledancer.dancers.dancer import IDancer
from tabledancer.models.table_spec import TableSpec


class DatabricksTableSpec(TableSpec):
    def __init__(self, name: str, columns: List[Any]) -> None:
        super().__init__(name, columns)


class DatabricksDancer(IDancer):
    def __init__(
        self, workspace_id: str, token: str, cluster_id: str, port: str
    ) -> None:
        super().__init__()

    def dance(self):
        pass

    def parse_table_spec(self, table_spec_dict: Dict[str, Any]) -> TableSpec:
        raise NotImplementedError()
        return DatabricksTableSpec(None, None)
