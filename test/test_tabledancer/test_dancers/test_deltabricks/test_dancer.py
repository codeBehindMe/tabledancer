from typing import Any, Dict
from tabledancer.utils.misc import read_yaml_file
from tabledancer.dancers.deltabricks.dancer import DeltabricksBackend, DeltabricksDancer, DeltabricksTableSpec

import pytest

TEST_YAML_FILE_PATH = "test/resources/basic_db_table.yaml"

@pytest.fixture
def simple_choreograph(scope="function") -> Dict[str, Any]:
  return read_yaml_file(TEST_YAML_FILE_PATH)
  

@pytest.mark.usefixtures('simple_choreograph')
class TestDeltabricksTableSpec:

  def test_from_dict_parses_correctly(self, simple_choreograph: Dict[str, Any]):

    table_spec = simple_choreograph['table_spec']

    want = DeltabricksTableSpec(table_name="simple_table"
    , database_name="myproject"
    , columns=[{"featureOne":{"type": "int", "comment": "It's a feature"}}
    , {"featureTwo":{"type": "string", "comment": "It's another feature"}}]
    , using="DELTA")

    got = DeltabricksTableSpec.from_dict(table_spec)

    assert got.table_name == want.table_name
    assert got.database_name == want.database_name
    assert got.columns == want.columns
    assert got.using == want.using

