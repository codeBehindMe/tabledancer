from typing import Any, Dict, List

import pytest

from tabledancer.dancers.dancer import IDancer
from tabledancer.models.table_spec import TableSpec
from tabledancer.parsers.parser import Parser
from tabledancer.utils.misc import read_yaml_file


@pytest.fixture(scope="class")
def simple_table_dict() -> Dict[str, Any]:
    return read_yaml_file("test/resources/simple_table.yaml")


@pytest.fixture(scope="class")
def dummy_dancer() -> IDancer:
    class DummyTableSpec(TableSpec):
        def __init__(self, name: str, columns: List[Any]) -> None:
            super().__init__(name, columns)

        def diff(other: TableSpec) -> Any:
            return super().diff()

        def is_diff(self, other: TableSpec) -> bool:
            return super().is_diff(other)

    class DummyDancer(IDancer):
        def __init__(self) -> None:
            super().__init__()

        def dance(self):
            return super().dance()

        def parse_table_spec(self, table_spec_dict: Dict[str, Any]) -> TableSpec:
            return DummyTableSpec(table_spec_dict["name"], table_spec_dict["columns"])

        def get_table_ddl_from_backend(self, table_spec: TableSpec) -> str:
            return super().get_table_ddl_from_backend(table_spec)

    return DummyDancer()


@pytest.mark.usefixtures("simple_table_dict", "dummy_dancer")
class TestParser:
    def test_parse_life_cycle_spec_dict(
        self, simple_table_dict: Dict[str, Any], dummy_dancer: IDancer
    ):

        parser = Parser(dummy_dancer)

        parser.parse_life_cycle_spec_dict(simple_table_dict)
