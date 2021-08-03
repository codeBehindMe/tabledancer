from fire import Fire

from tabledancer.dancers.dancer import IDancer
from tabledancer.dancers.databricks.dancer import DatabricksDancer


def _databricks_dancer_handler(**kwargs) -> DatabricksDancer:
    try:
        host = kwargs["host"]
        token = kwargs["token"]
        cluster_id = kwargs["cluster-id"]
        port = kwargs["port"]
        return DatabricksDancer(host, token, cluster_id, port)
    except KeyError as e:
        print(f"Must provide {e}")


dancers = {"databricks": _databricks_dancer_handler}


class DanceClass:
    def dance(self, path_to_spec: str, dancer: IDancer, **kwargs):
        dance_runner = dancers[dancer](**kwargs)


def app():
    return Fire(DanceClass)
