from typing import Any, Dict

from tabledancer.models.lifecycle_policy import LifeCyclePolicy
from tabledancer.models.table_spec import TableSpec


class LifeCycleSpec:
    def __init__(
        self,
        backend: str,
        life_cycle_policy: LifeCyclePolicy,
        table_spec: TableSpec,
    ) -> None:
        # FIXME: Docstring

        self.backend = backend
        self.life_cycle_policy = life_cycle_policy
        self.table_spec = table_spec
