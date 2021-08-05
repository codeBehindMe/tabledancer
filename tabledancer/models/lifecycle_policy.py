from typing import Any, Dict


class LifeCyclePolicy:
    def __init__(self) -> None:
        """Baseclass for lifecycle policies

        LifeCyclePolicies define how a table should be managed when there are
        changes to its schema.
        """
        self.properties = {}

    def get_properties(self) -> Dict[str, Any]:
        """Returns the properties associated with the policy.

        Returns:
            Dict[str, Any]: Dictionary of property keys and property values.
        """
        return self.properties


class DropCreateOnSchemaChange(LifeCyclePolicy):
    def __init__(self) -> None:
        """Drops the table and recreates table if the schemas are different.

        This policy will drop the table where the table schema defined in the
        yaml file is different to what is already in the database.
        """
        super().__init__()

    def get_properties(self) -> Dict[str, Any]:
        """Returns the properties associated with the policy.

        Returns:
            Dict[str, Any]: Dictionary of property keys and property values.
        """
        return super().get_properties()


class ErrorOnSchemaChange(LifeCyclePolicy):
    def __init__(self) -> None:
        """Raises an error if the table schemas are different.

        This policy will raise an exception if the table schema defined in the
        yaml file is different to what is already in the database.
        """
        super().__init__()

    def get_properties(self) -> Dict[str, Any]:
        """Returns the properties associated with the policy.

        Returns:
            Dict[str, Any]: Dictionary of property keys and property values.
        """
        return super().get_properties()


class EvolveOnSchemaChange(LifeCyclePolicy):
    def __init__(self, enable_remove_columns: bool) -> None:
        """Evolves the schema to match the schema defined in the yaml file.

        This policy attempt to use schema evolution if the plugin supports it.
        Args:
            enable_remove_columns (bool): This allows the schema evolution to
                remove existing columns in a table and consequently the data in
                these columns.
        """
        super().__init__()
        self.properties["enable_remove_columns"] = enable_remove_columns

    def get_properties(self) -> Dict[str, Any]:
        """Returns the properties associated with the policy.

        Returns:
            Dict[str, Any]: Dictionary of property keys and property values.
        """
        return super().get_properties()


life_cycle_policies = {
    DropCreateOnSchemaChange.__name__: DropCreateOnSchemaChange,
    ErrorOnSchemaChange.__name__: ErrorOnSchemaChange,
    EvolveOnSchemaChange.__name__: EvolveOnSchemaChange,
}
