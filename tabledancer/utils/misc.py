from os import path, truncate

import yaml


def read_yaml_file(path_to_yaml_file: str, allow_unsafe: bool = False):
    """Loads a yaml file to a dictionary from disk.

    Args:
        path_to_yaml_file (str): Path to the yaml file.
        allow_unsafe (bool, optional): Unsafe mode allows to load complex yaml
          tags. Defaults to False.

    Returns:
        [type]: [description]
    """

    with open(path_to_yaml_file, "r") as f:
        if allow_unsafe is True:
            return yaml.load(f)
        return yaml.safe_load(f)
