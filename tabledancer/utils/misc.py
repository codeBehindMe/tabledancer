from typing import Any, Dict

import yaml


def read_yaml_file(
    path_to_yaml_file: str, allow_unsafe: bool = False
) -> Dict[str, Any]:
    """Loads a yaml file to a dictionary from disk.

    Args:
        path_to_yaml_file (str): Path to the yaml file.
        allow_unsafe (bool, optional): Unsafe mode allows to load complex yaml
          tags. Defaults to False.

    Returns:
        Dict[str,Any]: Dictionary with the YAML file contents.
    """

    with open(path_to_yaml_file, "r") as f:
        if allow_unsafe is True:
            return yaml.load(f)
        return yaml.safe_load(f)


def is_none_or_empty_string(s: str) -> bool:
    """Checks if a given string is None or just whitespace string.

    Args:
        s (str): String to check

    Returns:
        bool: True if s is None or whitespace, else False.
    """

    if s is None:
        return True
    if len(s.strip()) == 0:
        return True
    return False
