from tabledancer.dancers.deltabricks.dancer import DeltabricksDancer
from tabledancer.utils.misc import read_yaml_file
from fire import Fire

from tabledancer.dancers.dancer import IDancer
from tabledancer.dancers.databricks.dancer import DatabricksDancer


def _databricks_dancer_handler(**kwargs) -> DatabricksDancer:
    # FIXME: Docstring
    try:
        host = kwargs["host"]
        token = kwargs["token"]
        cluster_id = kwargs["cluster-id"]
        port = kwargs["port"]
        return DatabricksDancer(host, token, cluster_id, port)
    except KeyError as e:
        print(f"Must provide {e}")


dancers = {"databricks": _databricks_dancer_handler
            , "deltabricks": DeltabricksDancer}


class DanceStudio:
    def dance(self, path_to_spec: str, **kwargs):
        # FIXME: Docstring
    
        choreograph = read_yaml_file(path_to_spec)

        dancer = dancers[choreograph['backend']](**kwargs)
        dancer.dance(choreograph)

def app():
    # FIXME: Docstring
    return Fire(DanceStudio)
