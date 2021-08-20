import sys

from fire import Fire

from tabledancer.dancers.deltabricks.dancer import DeltabricksDancer
from tabledancer.dancers.deltaspark.dancer import DeltaSparkDancer
from tabledancer.utils.logger import app_logger
from tabledancer.utils.misc import read_yaml_file

dancers = {
    "databricks": DeltabricksDancer.with_default_backend,
    "deltaspark": DeltaSparkDancer,
}


class DanceStudio:
    def dance(self, path_to_spec: str, **kwargs):
        # FIXME: Docstring

        app_logger.info(f"dancing {path_to_spec}")
        choreograph = read_yaml_file(path_to_spec)

        backend = choreograph["backend"]
        try:
            dancer = dancers[backend](**kwargs)
        except KeyError:
            app_logger.error(f"unsupported backend: {backend}")
            sys.exit(1)
        dancer.dance(choreograph)


def app():
    # FIXME: Docstring
    return Fire(DanceStudio)
