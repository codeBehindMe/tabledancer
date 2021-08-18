from fire import Fire

from tabledancer.dancers.deltabricks.dancer import DeltabricksDancer
from tabledancer.dancers.deltaspark.dancer import DeltaSparkDancer
from tabledancer.utils.misc import read_yaml_file

dancers = {
    "databricks": DeltabricksDancer.with_default_backend,
    "deltaspark": DeltaSparkDancer,
}


class DanceStudio:
    def dance(self, path_to_spec: str, **kwargs):
        # FIXME: Docstring

        choreograph = read_yaml_file(path_to_spec)

        dancer = dancers[choreograph["backend"]](**kwargs)
        dancer.dance(choreograph)


def app():
    # FIXME: Docstring
    return Fire(DanceStudio)
