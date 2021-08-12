import os
import shutil
from test.test_tabledancer.test_dancers.test_databricks._fixtures import (
    db_life_cycle_spec_dict, db_table_spec_dict)
from typing import Any, Dict

import pyspark
import pytest
from delta import configure_spark_with_delta_pip

from tabledancer.dancers.databricks.dancer import DatabricksDancer
from tabledancer.dancers.databricks.table_spec import DatabricksTableSpec
from tabledancer.models.lifecycle_spec import LifeCycleSpec
from tabledancer.models.table_spec import TableSpec
from tabledancer.utils.misc import read_yaml_file

builder = (
    pyspark.sql.SparkSession.builder.appName("tests")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()


@pytest.mark.usefixtures("db_table_spec_dict", "db_life_cycle_spec_dict")
class TestDatabricksDancer:
    def test_parse_table_spec_returns_table_spec(
        self, db_table_spec_dict: Dict[str, Any]
    ):
        """Checks that the parse_table_spec function returns a TableSpec object.

        Args:
            db_table_spec_dict (Dict[str,Any]): Databricks table spec dictionary.
        """

        db_dancer = DatabricksDancer(None, None, None, None)

        assert isinstance(db_dancer.parse_table_spec(db_table_spec_dict), TableSpec)

    def test_parse_life_cycle_spec_returns_life_cycle_spec(
        self, db_life_cycle_spec_dict: Dict[str, Any]
    ):
        """Checks that the parse_life_cycle_spec returns a LifeCycleSpec object.

        Args:
            db_life_cycle_spec_dict (Dict[str, Any]): Dictionary of the life
                cycle spec.
        """
        db_dancer = DatabricksDancer(None, None, None, None)

        assert isinstance(
            db_dancer._parse_life_cycle_spec_dict(db_life_cycle_spec_dict),
            LifeCycleSpec,
        )

    def test_table_does_not_exists_move(self, db_table_spec_dict: Dict[str, Any]):
        # FIXME: Docstring
        db_dancer = DatabricksDancer(None, None, None, None)

        table_spec = DatabricksTableSpec(
            name="mytable",
            database="default",
            columns=[("a", "int"), ("b", "string")],
            using="delta",
        )

        db_dancer.table_does_not_exist_move(table_spec)
        if os.path.isdir("spark-warehouse"):
            shutil.rmtree("spark-warehouse")

    def test_error_on_schema_change_raises_value_error(self):
        # FIXME: Docstring

        with pytest.raises(ValueError) as e:
            assert e == DatabricksDancer(
                None, None, None, None
            ).error_on_schema_change_move(None)

    def test_evolve_on_schema_change_move(self):
        # FIXME: Docstring
        with pytest.raises(ValueError) as e:
            assert e == DatabricksDancer(
                None, None, None, None
            ).evolve_on_schema_change_move(None, None)
