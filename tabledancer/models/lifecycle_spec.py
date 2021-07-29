from typing import Any, Dict


class LifeCycleSpec:
    def __init__(
        self,
        name: str,
        backend: str,
        database_name: str,
        life_cycle_policy: Dict[str, Any],
        table_spec: Dict[str, Any],
    ) -> None:

        self.name = name
        self.backend = backend
        self.database_name = database_name
        self.life_cycle_policy = life_cycle_policy
        self.table_spec = table_spec
