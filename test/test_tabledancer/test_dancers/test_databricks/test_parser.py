import pytest

from tabledancer.dancers.databricks.parser import DatabricksDDLParser


@pytest.fixture(scope="class")
def simple_ddl() -> str:
    return "CREATE TABLE `gth_prediction`.`predictions` (\n  `policy_id` STRING,\n  `probability` DOUBLE,\n  `prediction` INT,\n  `end_of_month` DATE,\n  `run_id` BIGINT)\nUSING delta\nOPTIONS (\n  `overwriteSchema` 'true')\nTBLPROPERTIES (\n  'overwriteSchema' = 'true')\n"


@pytest.mark.usefixtures("simple_ddl")
class TestDatabricksDDLParser:
    def test_extract_database_name_gets_correct_db_name(self, simple_ddl: str):
        ddl_parser = DatabricksDDLParser(simple_ddl)

        assert ddl_parser._get_database_name() == "gth_prediction"

    def test_extract_table_name(self, simple_ddl: str):
        ddl_parser = DatabricksDDLParser(simple_ddl)

        assert ddl_parser._get_table_name() == "predictions"
