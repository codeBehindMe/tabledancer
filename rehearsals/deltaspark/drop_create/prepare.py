from os.path import abspath

import pyspark
from delta import *

builder = (
    pyspark.sql.SparkSession.builder.appName(__name__)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.warehouse.dir", abspath("spark-warehouse"))
    .enableHiveSupport()
)
spark = spark = configure_spark_with_delta_pip(builder).getOrCreate()

if __name__ == "__main__":

    spark.sql("create database myproject")
