from test.test_tabledancer.test_dancers.test_databricks._fixtures import \
    db_table_spec_dict, db_life_cycle_spec_dict
from typing import Any, Dict

import pytest

from tabledancer.dancers.databricks.dancer import DatabricksDancer
from tabledancer.models.table_spec import TableSpec
from tabledancer.models.lifecycle_spec import LifeCycleSpec
from tabledancer.utils.misc import read_yaml_file


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

    def test_parse_life_cycle_spec_returns_life_cycle_spec(self, db_life_cycle_spec_dict: Dict[str, Any]):
        """Checks that the parse_life_cycle_spec returns a LifeCycleSpec object.

        Args:
            db_life_cycle_spec_dict (Dict[str, Any]): Dictionary of the life 
                cycle spec.
        """
        db_dancer = DatabricksDancer(None, None, None, None)

        assert isinstance(db_dancer._parse_life_cycle_spec_dict(db_life_cycle_spec_dict), LifeCycleSpec)