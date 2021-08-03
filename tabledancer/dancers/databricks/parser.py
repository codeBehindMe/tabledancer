import re
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
        db_search = re.search("(?<=TABLE `)(.*)(?=`\.)", self.ddl, re.IGNORECASE)
        if db_search:
            return db_search.group(1)
        raise ValueError("Could not extract db name")

    def _get_table_name(self) -> str:
        # FIXME: Docstring
        table_search = re.search("(?<=\.`)(.*)(?=` \()", self.ddl, re.IGNORECASE)
        if table_search:
            return table_search.group(1)
        raise ValueError("could not extract table name")

    def _find_ddl_definition_style(self, ddl: str):
        # FIXME: Docstring

        if ddl.find(_TABLE_DEFINITION_STYLE["external"]) != -1:
            self.table_definition_style = _TABLE_DEFINITION_STYLE["external"]
        elif ddl.find(_TABLE_DEFINITION_STYLE["managed"]) != -1:
            self.table_definition_style = _TABLE_DEFINITION_STYLE["managed"]
        else:
            raise ValueError("Unsupported table ddl")
