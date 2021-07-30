from typing import Dict

import pytest

from tabledancer.utils.misc import read_yaml_file
from tabledancer.utils.misc import is_none_or_empty_string


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


class TestIsNoneOrEmptyString:

  def test_empty_string_returns_true(self):
    """
    Checks that empty string returns true.
    """

    assert is_none_or_empty_string("            ") is True

  def test_none_returns_true(self):
    """
    Checks that None returns true.
    """
    
    assert is_none_or_empty_string(None) is True


  def test_valid_string_returns_false(self):
    """
    Checks that valid strings return false.
    """
    assert is_none_or_empty_string("this is a valid string ") is False