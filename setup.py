from setuptools import find_packages, setup
from setuptools.command.install import install

from tabledancer.dancers.deltabricks.dancer import DeltabricksDancer

req_spec = {
    "deltaspark": ["pyspark", "delta-spark", "jinja2"],
    "databricks8.1": ["databricks==8.1", "jinja2"],
}

requirements = None


class InstallCommand(install):
    description = "Installs tabledancer"
    user_options = install.user_options + [
        ("dancetime=", None, "Specify the runtime to install"),
    ]

    def initialize_options(self) -> None:
        install.initialize_options(self)
        self.dancetime = None

    def finalize_options(self) -> None:
        print(self.dancetime)
        install.finalize_options(self)

    def run(self) -> None:
        try:
            requirements = req_spec[self.dancetime]
        except KeyError as ke:
            raise ValueError("Unsupported runtime").with_traceback(ke.__traceback__)

        install.run(self)


setup(
    name="tabledancer",
    packages=find_packages(exclude=["test"]),
    install_requires=requirements,
    entry_points={"console_scripts": ["tabledancer=tabledancer.main:app"]},
    cmdclass={"install": InstallCommand},
)
