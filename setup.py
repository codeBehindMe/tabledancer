import subprocess
import sys

from setuptools import find_packages, setup
from setuptools.command.install import install

BASE_REQUIREMENTS = ["fire", "jinja2", "pyyaml"]
req_spec = {
    "deltaspark": ["pyspark", "delta-spark"] + BASE_REQUIREMENTS,
    "databricks8.1": ["databricks-connect==8.1"] + BASE_REQUIREMENTS,
}


class InstallCommand(install):
    description = "Installs tabledancer"
    user_options = install.user_options + [
        ("style=", None, "Specify the style to install"),
    ]

    def initialize_options(self) -> None:
        install.initialize_options(self)
        self.style = None

    def finalize_options(self) -> None:
        install.finalize_options(self)

    def run(self) -> None:
        try:
            requirements = req_spec[self.style]
            for req in requirements:
                subprocess.check_call([sys.executable, "-m", "pip", "install", req])
        except KeyError as ke:
            raise ValueError("Unsupported runtime").with_traceback(ke.__traceback__)

        install.run(self)


setup(
    name="tabledancer",
    packages=find_packages(exclude=["test"]),
    entry_points={"console_scripts": ["tabledancer=tabledancer.main:app"]},
    cmdclass={"install": InstallCommand},
    package_data = {'':['*.j2']}
)
