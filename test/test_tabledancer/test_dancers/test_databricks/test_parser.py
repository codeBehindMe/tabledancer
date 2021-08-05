import pytest

from tabledancer.dancers.databricks.parser import DatabricksDDLParser
from tabledancer.dancers.databricks.table_spec import DatabricksTableSpec


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

    def test_extract_columns(self, simple_ddl: str):
        ddl_parser = DatabricksDDLParser(simple_ddl)

        expected = {
            ("end_of_month", "DATE"),
            ("run_id", "BIGINT"),
            ("probability", "DOUBLE"),
            ("prediction", "INT"),
            ("policy_id", "STRING"),
        }
        assert set(ddl_parser._get_columns()) == expected

    def test_parse(self, simple_ddl: str):

        ddl_string = "CREATE TABLE `mydatabase`.`mytable` (\n  `policy_id` STRING,\n  `probability` DOUBLE,\n  `prediction` INT,\n  `end_of_month` DATE,\n  `run_id` BIGINT)\nUSING delta\nOPTIONS (\n  `overwriteSchema` 'true')\nTBLPROPERTIES (\n  'overwriteSchema' = 'true')\n"

        ddl_parser = DatabricksDDLParser(ddl_string)

        want = DatabricksTableSpec(
            name="mytable",
            database="mydatabase",
            columns=[
                ("policy_id", "STRING"),
                ("probability", "DOUBLE"),
                ("prediction", "INT"),
                ("end_of_month", "DATE"),
                ("run_id", "BIGINT"),
            ],
        )

        got = ddl_parser.parse()
        assert got.name == want.name
        assert got.database == want.database
        assert set(got.columns) == set(want.columns)
