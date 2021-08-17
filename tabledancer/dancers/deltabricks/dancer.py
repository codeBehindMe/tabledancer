from __future__ import annotations

from functools import wraps
from typing import Any, Dict, Generator, List, Optional, Tuple

from pyspark.sql import SparkSession

from tabledancer.utils.misc import is_none_or_empty_string


class DeltabricksTableSpec:
    def __init__(
        self, table_name: str, database_name: str, columns: Dict[str, str], using: str
    ) -> None:
        self.table_name = table_name
        self.database_name = database_name
        self.columns = columns
        self.using = using

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> DeltabricksTableSpec:
        """Creates a TableSpec by parsing a dictionary.

        Args:
            d (Dict[str, Any]): Dictionary containing table spec data.

        Returns:
            DeltabricksTableSpec: The constructed object.
        """

        return DeltabricksTableSpec(
            table_name=d["name"],
            database_name=d["database"],
            columns=d["columns"],
            using=d["using"],
        )

    @staticmethod
    def from_ddl_info(ddl_info: Dict[str, Any]) -> DeltabricksTableSpec:

        columns = []
        for c_name, c_type, c_comment in ddl_info["columns"]:
            columns.append({c_name: {"type": c_type, "comment": c_comment}})

        return DeltabricksTableSpec(
            table_name=ddl_info["table"],
            database_name=ddl_info["database"],
            columns=columns,
            using=ddl_info["provider"],
        )

    def to_create_table_ddl(self):
        raise NotImplementedError()

    def is_same(self, other: DeltabricksTableSpec) -> bool:
        raise NotImplementedError()


class DeltabricksBackend:
    def __init__(self) -> None:
        self.spark = SparkSession.builder.getOrCreate()

    def sql(self, sql: str):
        return self.spark.sql(sql)

    def table_exists(self, database_name: str, table_name: str) -> bool:
        return self.spark._jsparkSession.catalog().tableExists(
            database_name, table_name
        )

    def get_ddl(self, database_name: str, table_name: str) -> str:

        return self.sql(f"SHOW TABLE EXTENDED IN {database_name} LIKE '{table_name}'")

    def _get_table_struct_info(self, db_name: str, table_name: str) -> Dict[str, Any]:
        struct_info = (
            self.spark.sql(f"SHOW TABLE EXTENDED in {db_name} LIKE '{table_name}'")
            .limit(1)
            .collect()[0]
            .information
        )
        tokens = filter(
            lambda x: not is_none_or_empty_string(x), struct_info.split("\n")
        )
        tokens = [str.lower(t) for t in tokens]
        return dict([x.strip() for x in t.split(":", 1)] for t in tokens)

    def _get_table_col_info(
        self, db_name: str, table_name: str
    ) -> Generator[Tuple[str, str, str]]:
        col_info = (
            self.spark.sql(f"DESCRIBE TABLE {db_name}.{table_name}")
            .where("data_type != null")
            .collect()
        )

        for row in col_info:
            yield row.col_name, row.data_type, row.comment

    def get_ddl_info(self, database_name: str, table_name: str) -> Dict[str, Any]:

        ddl_info = self._get_table_col_info(database_name, table_name)
        ddl_info["columns"] = list(self._get_table_col_info(database_name, table_name))

        return ddl_info


def action(m):
    @wraps(m)
    def _wrapper(self, *m_args, **m_kwargs):
        m_return = m(self, *m_args, **m_kwargs)
        return m_return

    return _wrapper


class DeltabricksDancer:
    def __init__(self, host: str, token: str, cluster_id: str, port: int) -> None:
        self.backend = DeltabricksBackend()

    @action
    def drop_create_on_schema_change(
        self,
        vc_table_spec: DeltabricksTableSpec,
        backend_table_spec: DeltabricksTableSpec,
        properties: Optional[Dict[str, Any]],
    ):
        self.backend.sql(
            f"DROP TABLE {vc_table_spec.database_name}.{vc_table_spec.table_name}"
        )
        self.backend.sql(vc_table_spec.to_create_table_ddl())

    @action
    def error_on_schema_change(
        self,
        vc_table_spec: DeltabricksTableSpec,
        backend_table_spec: DeltabricksTableSpec,
        properties: Optional[Dict[str, Any]],
    ):
        raise ValueError("Tables are different")

    def dance(self, choreograph: Dict[str, Any]):

        vc_table_spec = DeltabricksTableSpec.from_dict(choreograph["table_spec"])

        if not self.backend.table_exists(
            vc_table_spec.database_name, vc_table_spec.table_name
        ):
            self.backend.sql(vc_table_spec.to_create_table_ddl())

        backend_table_spec = DeltabricksTableSpec.from_ddl_info(
            self.backend.get_ddl(vc_table_spec.database_name, vc_table_spec.table_name)
        )

        if not vc_table_spec.is_same(backend_table_spec):

            action = getattr(self, choreograph["life_cycle_policy"]["name"])
            action(
                vc_table_spec,
                backend_table_spec,
                choreograph["life_cycle_policy"]["properties"],
            )
