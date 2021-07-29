from setuptools import find_packages, setup

setup(
    name="tabledancer",
    packages=find_packages(),
    entry_points={"console_scripts": ["tabledancer=tabledancer.main:hello"]},
)
