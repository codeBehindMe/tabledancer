from test.test_tabledancer.test_dancers.test_databricks._fixtures import \
    db_table_spec_dict
from typing import Any, Dict

import pytest

from tabledancer.dancers.databricks.table_spec import DatabricksTableSpec


@pytest.mark.usefixtures("db_table_spec_dict")
class TestDatabricksTableSpec:
    def test_name_cannot_be_none(self, db_table_spec_dict: Dict[str, Any]):
        """Check's that the name cannot be set to None

        Args:
            db_table_spec_dict (Dict[str, Any]): Simple databricks table spec dict.
        """

        db_table_spec_dict["name"] = None
        with pytest.raises(ValueError):
            DatabricksTableSpec(**db_table_spec_dict)

    def test_name_cannot_be_whitespace(self, db_table_spec_dict: Dict[str, Any]):
        """Checks that the name cannot be empty string or whitespace

        Args:
            db_table_spec_dict (Dict[str, Any]): Simple databricks table spec dict.
        """
        db_table_spec_dict["name"] = "                 "
        with pytest.raises(ValueError):
            DatabricksTableSpec(**db_table_spec_dict)

    def test_partitions_raises_not_implemented_error(
        self, db_table_spec_dict: Dict[str, Any]
    ):
        """Currently partitions field is not supported.

        Args:
            db_table_spec_dict (Dict[str, Any]): Simple databricks table spec dict.
        """
        db_table_spec_dict["partitions"] = []
        with pytest.raises(NotImplementedError):
            DatabricksTableSpec(**db_table_spec_dict)

    def test_location_raises_not_implemented_error(
        self, db_table_spec_dict: Dict[str, Any]
    ):
        """Currently the location field is not supported.

        Args:
            db_table_spec_dict (Dict[str, Any]): Simple databricks table spec dict.
        """
        db_table_spec_dict["location"] = "/path/to/table"
        with pytest.raises(NotImplementedError):
            DatabricksTableSpec(**db_table_spec_dict)

    def test_options_raises_not_implemented_error(
        self, db_table_spec_dict: Dict[str, Any]
    ):
        db_table_spec_dict["options"] = []
        with pytest.raises(NotImplementedError):
            DatabricksTableSpec(**db_table_spec_dict)
