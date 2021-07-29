from typing import Dict

import pytest

from tabledancer.utils.misc import read_yaml_file


@pytest.fixture(scope="class")
def test_yaml_file_path() -> str:
    return "test/resources/simple_table.yaml"


@pytest.mark.usefixtures("test_yaml_file_path")
class TestReadYaml:
    def test_read_yaml_returns_dict(self, test_yaml_file_path: str):
        """Check's that the read_yaml function returns a dict.

        Args:
            test_yaml_file_path (str): Path to the test yaml file.
        """

        assert isinstance(read_yaml_file(test_yaml_file_path), Dict)
