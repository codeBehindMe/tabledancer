from tabledancer.dancers.deltabricks.dancer import DeltabricksDancer

import pyspark
from os.path import abspath
from delta import configure_spark_with_delta_pip

class DeltaSparkDancer(DeltabricksDancer):
  
  def __init__(self) -> None:
      super().__init__()

      builder = (
      pyspark.sql.SparkSession.builder.appName("tests")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
          "spark.sql.catalog.spark_catalog",
          "org.apache.spark.sql.delta.catalog.DeltaCatalog",
      )
      .config("spark.sql.warehouse.dir", abspath("spark-warehouse"))
      .enableHiveSupport())
      self.backend.spark = configure_spark_with_delta_pip(builder).getOrCreate()
