from typing import Any, Dict

import pytest

from tabledancer.utils.misc import read_yaml_file

TEST_YAML_FILE_PATH = "test/resources/basic_db_table.yaml"

@pytest.fixture(scope="function")
def db_table_spec_dict() -> Dict[str, Any]:
    return read_yaml_file(TEST_YAML_FILE_PATH)["table_spec"]

@pytest.fixture(scope="function")
def db_life_cycle_spec_dict() -> Dict[str, Any]:
    return read_yaml_file(TEST_YAML_FILE_PATH)