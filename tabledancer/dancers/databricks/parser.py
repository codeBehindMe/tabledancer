import re
from typing import Any, List, Optional

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

        return DatabricksTableSpec(
            name=self._get_table_name(),
            database=self._get_database_name(),
            columns=self._get_columns(),
        )

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

    # FIXME: Refactor tuple (name, type) to a proper type.
    def _get_columns(self) -> List[Any]:
        # FIXME: Docstring
        columns_str = self.ddl.split("(")[1].split(")")[0].replace("\n", "").strip()
        columns_str_lists = map(lambda x: x.strip(), columns_str.split(","))

        name_type_split = [x.split(" ") for x in columns_str_lists]
        removed_backtick = [(x[0].replace("`", ""), x[1]) for x in name_type_split]

        return removed_backtick

    def _get_comment(self):
        # FIXME: Docstring
        return None  # FIXME: Shouldn't be None

    def _get_using(self):
        # FIXME: Docstring
        return None  # FIXME: Souldn't be None

    def _find_ddl_definition_style(self, ddl: str):
        # FIXME: Docstring

        if ddl.find(_TABLE_DEFINITION_STYLE["external"]) != -1:
            self.table_definition_style = _TABLE_DEFINITION_STYLE["external"]
        elif ddl.find(_TABLE_DEFINITION_STYLE["managed"]) != -1:
            self.table_definition_style = _TABLE_DEFINITION_STYLE["managed"]
        else:
            raise ValueError("Unsupported table ddl")
