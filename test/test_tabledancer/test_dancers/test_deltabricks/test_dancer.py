import os
import shutil
from typing import Any, Dict

import pyspark
import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.session import SparkSession

from tabledancer.dancers.deltabricks.dancer import (DeltabricksBackend,
                                                    DeltabricksDancer,
                                                    DeltabricksTableSpec)
from tabledancer.utils.misc import read_yaml_file

TEST_YAML_FILE_PATH = "test/resources/basic_db_table.yaml"


@pytest.fixture
def simple_choreograph(scope="function") -> Dict[str, Any]:
    return read_yaml_file(TEST_YAML_FILE_PATH)


@pytest.fixture(scope="function")
def spark() -> SparkSession:

    if os.path.isdir("spark-warehouse"):
        shutil.rmtree("spark-warehouse")

    builder = (
        pyspark.sql.SparkSession.builder.appName("tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    yield spark

    spark.stop()

    if os.path.isdir("spark-warehouse"):
        shutil.rmtree("spark-warehouse")


@pytest.mark.usefixtures("spark")
class TestDeltabricksBackend:
    def test_check_if_table_exists_returns_bool(self, spark: SparkSession):

        ddl = """CREATE TABLE myproject.simple_table (
          featureOne int COMMENT "It's a feature"
          , featureTwo string COMMENT "It's another feature"
          ) 
        USING DELTA
        """
        backend = DeltabricksBackend()
        backend.sql("CREATE DATABASE myproject")

        backend.sql(ddl)

        assert backend.table_exists("myproject", "simple_table")

    def test_get_ddl_info_returns_dict(self, spark: SparkSession):

        ddl = """CREATE TABLE myprojecttwo.simple_table (
          featureOne int COMMENT "It's a feature"
          , featureTwo string COMMENT "It's another feature"
          ) 
        USING DELTA
        """
        backend = DeltabricksBackend()
        backend.sql("CREATE DATABASE myprojecttwo")

        backend.sql(ddl)

        ddl_info = backend.get_ddl_info("myprojecttwo", "simple_table")

        assert isinstance(ddl_info, Dict)
        assert ddl_info["table"] == "simple_table"
        assert ddl_info["database"] == "myprojecttwo"
        assert ddl_info["provider"].lower() == "delta"

        assert ddl_info["columns"][0] == ("featureOne", "int", "It's a feature")
        assert ddl_info["columns"][1] == (
            "featureTwo",
            "string",
            "It's another feature",
        )


@pytest.mark.usefixtures("simple_choreograph", "spark")
class TestDeltabricksTableSpec:
    def test_from_dict_parses_correctly(self, simple_choreograph: Dict[str, Any]):

        table_spec = simple_choreograph["table_spec"]

        want = DeltabricksTableSpec(
            table_name="simple_table",
            database_name="myproject",
            columns=[
                {"featureOne": {"type": "int", "comment": "It's a feature"}},
                {"featureTwo": {"type": "string", "comment": "It's another feature"}},
            ],
            using="DELTA",
        )

        got = DeltabricksTableSpec.from_dict(table_spec)

        assert got.table_name == want.table_name
        assert got.database_name == want.database_name
        assert got.columns == want.columns
        assert got.using == want.using

    def test_from_ddl_info(self, spark: SparkSession):
        """Checks that the from ddl method is correct"""

        spark.sql("drop database if exists myproject cascade")
        spark.sql("CREATE DATABASE myprojectone")

        ddl = """CREATE TABLE myprojectone.simple_table (
         featureOne int COMMENT "It's a feature"
          , featureTwo string COMMENT "It's another feature"
          ) 
        USING DELTA
        """

        want = DeltabricksTableSpec(
            table_name="simple_table",
            database_name="myprojectone",
            columns=[
                {"featureOne": {"type": "int", "comment": "It's a feature"}},
                {"featureTwo": {"type": "string", "comment": "It's another feature"}},
            ],
            using="delta",
        )

        spark.sql(ddl)

        ddl_info = DeltabricksBackend().get_ddl_info("myprojectone", "simple_table")

        got = DeltabricksTableSpec.from_ddl_info(ddl_info)

        assert got.table_name == want.table_name
        assert got.database_name == want.database_name
        assert got.columns == want.columns
        assert got.using == want.using
