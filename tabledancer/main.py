import sys

from fire import Fire

from tabledancer.utils.logger import app_logger
from tabledancer.utils.misc import read_yaml_file


def make_dancer(dancer_name: str) -> object:
    if dancer_name == "databricks":
        from tabledancer.dancers.deltabricks.dancer import DeltabricksDancer

        return DeltabricksDancer.with_default_backend
    if dancer_name == "deltaspark":
        from tabledancer.dancers.deltaspark.dancer import DeltaSparkDancer

        return DeltaSparkDancer
    raise ValueError("unsupported dancer")


class DanceStudio:
    def dance(self, path_to_spec: str, **kwargs):
        # FIXME: Docstring

        app_logger.info(f"dancing {path_to_spec}")
        choreograph = read_yaml_file(path_to_spec)

        backend = choreograph["backend"]
        try:
            dancer = make_dancer(backend)(**kwargs)
        except ValueError:
            app_logger.error(f"unsupported backend: {backend}")
            sys.exit(1)
        dancer.dance(choreograph)


def app():
    # FIXME: Docstring
    return Fire(DanceStudio)
