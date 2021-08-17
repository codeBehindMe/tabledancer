
from __future__ import annotations
from typing import Any, Dict, Optional
from pyspark.sql import SparkSession
from functools import wraps


class DeltabricksTableSpec:

  def __init__(self, table_name : str, database_name: str, columns : Dict[Any]) -> None:
      self.table_name = table_name
      self.database_name = database_name
      self.columns = columns

  @staticmethod
  def from_dict(d : Dict[str, Any]) -> DeltabricksTableSpec:
    raise NotImplementedError()

  @staticmethod
  def from_ddl(ddl: str) -> DeltabricksTableSpec:
    raise NotImplementedError()

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


def action(m):
  @wraps(m)
  def _wrapper(self, *m_args, **m_kwargs):
    m_return = m(self, *m_args,**m_kwargs)
    return m_return
  return _wrapper




class DeltabricksDancer():

  def __init__(self, host : str, token: str, cluster_id : str, port : int) -> None:
      self.backend = DeltabricksBackend()

  @action
  def drop_create_on_schema_change(self, vc_table_spec : DeltabricksTableSpec, backend_table_spec: DeltabricksTableSpec, properties : Optional[Dict[str, Any]]):
    self.backend.sql(f"DROP TABLE {vc_table_spec.database_name}.{vc_table_spec.table_name}")
    self.backend.sql(vc_table_spec.to_create_table_ddl())

  @action
  def error_on_schema_change(self,vc_table_spec : DeltabricksTableSpec, backend_table_spec: DeltabricksTableSpec, properties: Optional[Dict[str, Any]]):
    raise ValueError("Tables are different")

  def dance(self, choreograph: Dict[str, Any]):
    
    vc_table_spec = DeltabricksTableSpec.from_dict(choreograph['table_spec'])

    if not self.backend.table_exists(vc_table_spec.database_name,vc_table_spec.table_name):
      self.backend.sql(vc_table_spec.to_create_table_ddl())

    backend_table_spec = DeltabricksTableSpec.from_ddl(self.backend.get_ddl(vc_table_spec.database_name,vc_table_spec.table_name))

    if not vc_table_spec.is_same(backend_table_spec):
      
      action = getattr(self,choreograph['life_cycle_policy']['name'])
      action(vc_table_spec, backend_table_spec, choreograph['life_cycle_policy']['properties'])




  