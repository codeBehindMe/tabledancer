from typing import Any, Dict

import pytest

from tabledancer.utils.misc import read_yaml_file


@pytest.fixture(scope="function")
def db_table_spec_dict() -> Dict[str, Any]:
    return read_yaml_file("test/resources/basic_db_table.yaml")["table_spec"]
