from tabledancer.models.lifecycle_policy import ErrorOnSchemaChange
from setuptools import find_packages, setup

setup(
    name="tabledancer",
    packages=find_packages(exclude=['test']),
    entry_points={"console_scripts": ["tabledancer=tabledancer.main:hello"]},
)
