import re
from typing import Any, Final, List, Optional

from tabledancer.dancers.databricks.table_spec import DatabricksTableSpec
from tabledancer.utils.templating import Templater

_TABLE_DEFINITION_STYLE = {
    "managed": "CREATE TABLE",
    "external": "CREATE EXTERNAL TABLE",
}

PATH_TO_TEMPLATES: Final[str] = "tabledancer/databricks/templates/"
CREATE_TABLE_TEMPLATE: Final[str] = "create_table.sql.j2"


class DatabricksDDLParser:
    def __init__(self) -> None:
        # FIXME: Docstring
        self.table_definition_style: Optional[str] = None

    def to_table_spec(self, ddl_str: str) -> DatabricksTableSpec:
        # FIXME: Docstring

        return DatabricksTableSpec(
            name=self._get_table_name(ddl_str),
            database=self._get_database_name(ddl_str),
            columns=self._get_columns(ddl_str),
        )

    def to_ddl(self, table_spec: DatabricksTableSpec) -> str:
        # FIXME: Docstring
        name = table_spec.name
        columns = table_spec.columns
        using = table_spec.using
        return Templater(PATH_TO_TEMPLATES).render_template(
            CREATE_TABLE_TEMPLATE, name=name, columns=columns, using=using
        )

    def _get_database_name(self, ddl_str: str) -> str:
        # FIXME: Docstring
        db_search = re.search(r"(?<=TABLE `)(.*)(?=`\.)", ddl_str, re.IGNORECASE)
        if db_search:
            return db_search.group(1)
        raise ValueError("Could not extract db name")

    def _get_table_name(self, ddl_str: str) -> str:
        # FIXME: Docstring
        table_search = re.search(r"(?<=\.`)(.*)(?=` \()", ddl_str, re.IGNORECASE)
        if table_search:
            return table_search.group(1)
        raise ValueError("could not extract table name")

    # FIXME: Refactor tuple (name, type) to a proper type.
    def _get_columns(self, ddl_str: str) -> List[Any]:
        # FIXME: Docstring
        columns_str = ddl_str.split("(")[1].split(")")[0].replace("\n", "").strip()
        columns_str_lists = map(lambda x: x.strip(), columns_str.split(","))

        name_type_split = [x.split(" ") for x in columns_str_lists]
        removed_backtick = [(x[0].replace("`", ""), x[1]) for x in name_type_split]

        return removed_backtick

    def _get_comment(self, ddl_str: str):
        # FIXME: Docstring
        return None  # FIXME: Shouldn't be None

    def _get_using(self, ddl_str: str):
        # FIXME: Docstring
        return None  # FIXME: Souldn't be None

    def _find_ddl_definition_style(self, ddl_str: str):
        # FIXME: Docstring

        if ddl_str.find(_TABLE_DEFINITION_STYLE["external"]) != -1:
            self.table_definition_style = _TABLE_DEFINITION_STYLE["external"]
        elif ddl_str.find(_TABLE_DEFINITION_STYLE["managed"]) != -1:
            self.table_definition_style = _TABLE_DEFINITION_STYLE["managed"]
        else:
            raise ValueError("Unsupported table ddl")
