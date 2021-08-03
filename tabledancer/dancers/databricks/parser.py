from typing import Optional

from tabledancer.dancers.databricks.table_spec import DatabricksTableSpec

_TABLE_DEFINITION_STYLE = {
    "managed": "CREATE TABLE",
    "external": "CREATE EXTERNAL TABLE",
}


class DatabricksDDLParser:
    def __init__(self, ddl: str) -> None:
        # FIXME: Docstring
        self.table_definition_style: Optional[str] = None
        self.ddl = ddl

    def parse(self) -> DatabricksTableSpec:
        # FIXME: Docstring
        pass

    def _get_database_name(self) -> str:
        # FIXME: Docstring
        ct_end_pos = self.ddl.find(self.table_definition_style) + len(
            self.table_definition_style
        )
        period_pos = self.ddl.find(".")

    def _find_ddl_definition_style(self, ddl: str):
        # FIXME: Docstring

        if ddl.find(_TABLE_DEFINITION_STYLE["external"]) != -1:
            self.table_definition_style = _TABLE_DEFINITION_STYLE["external"]
        elif ddl.find(_TABLE_DEFINITION_STYLE["managed"]) != -1:
            self.table_definition_style = _TABLE_DEFINITION_STYLE["managed"]
        else:
            raise ValueError("Unsupported table ddl")
