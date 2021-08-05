import pytest

from tabledancer.utils.string import find_first_between


@pytest.fixture(scope="class")
def test_string() -> str:
    return "create table `db_name`.`table_name`"


@pytest.mark.usefixtures("test_string")
class TestFindFirstBetween:
    def test_finds_correct_index(self, test_string: str):
        """
        Checks that the index in a substring find is correct.

        Aim is to find the index of the first backtick (`) which is index 13.

        Args:
          test_string (str): String to test.
        """

        assert find_first_between(test_string, "`", 11, 21) == 13

    def test_outside_left_bound(self, test_string: str):
        """
        Checks that if token is left of the window it returns -1

        Args:
          test_string (str): String to test.
        """

        assert find_first_between(test_string, "`", 14, 19) == -1

    def test_outside_right_bound(self, test_string: str):
        """
        Checks that the token is right of the window it returns -1

        Args:
            test_string (str): String to test.
        """

        assert find_first_between(test_string, "`", 0, 11) == -1
