import pytest

from tabledancer.utils.templating import Templater


class TestTemplater:
    def test_render_basic_table(self):
        # FIXME: Docstring
        t = Templater("test/resources/databricks_templates")
        want = """CREATE  TABLE  mytable
(
  
    a int  , 
  
    b string  
  
)
USING DELTA;"""
        got = t.render_template(
            "basic_table.sql.j2",
            external=None,
            name="mytable",
            columns=[("a", "int"), ("b", "string")],
            using="DELTA",
        )
        assert got == want
