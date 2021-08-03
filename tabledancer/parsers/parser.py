from typing import Any, Dict

from tabledancer.dancers.dancer import IDancer
from tabledancer.models.lifecycle_policy import (LifeCyclePolicy,
                                                 life_cycle_policies)
from tabledancer.models.lifecycle_spec import LifeCycleSpec
from tabledancer.models.table_spec import TableSpec


class Parser:
    def __init__(self, dancer: IDancer) -> None:
        # FIXME: Docstring
        self.dancer = dancer

    def parse_life_cycle_spec_dict(
        self, life_cycle_spec_dict: Dict[str, Any]
    ) -> LifeCycleSpec:
    # FIXME: Docstring

        life_cycle_spec_dict["life_cycle_policy"] = self._parse_life_cycle_policy_dict(
            life_cycle_spec_dict["life_cycle_policy"]
        )
        life_cycle_spec_dict["table_spec"] = self._parse_table_spec_dict(
            life_cycle_spec_dict["table_spec"]
        )
        return LifeCycleSpec(**life_cycle_spec_dict)

    def _parse_life_cycle_policy_dict(
        self, life_cycle_policy_dict: Dict[str, Any]
    ) -> LifeCyclePolicy:
    # FIXME: Docstring

        # FIXME [Aaron]: Strings in code which are reused in YAML files and
        #   possibly other locations in the future.
        policy_name = life_cycle_policy_dict["policy"]
        policy_properties = life_cycle_policy_dict["properties"]

        if policy_properties is not None:
            return life_cycle_policies[policy_name](**policy_properties)
        return life_cycle_policies[policy_name]()

    def _parse_table_spec_dict(self, table_spec_dict: Dict[str, Any]) -> TableSpec:
        # FIXME: Docstring

        return self.dancer.parse_table_spec(table_spec_dict)
