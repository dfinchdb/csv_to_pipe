"""
setup.py configuration script describing how to build and package this project.

This file is primarily used by the setuptools library and typically should not
be executed directly. See README.md for how to deploy, test, and run
the csv_to_pipe project.
"""
from setuptools import setup, find_packages

import sys

sys.path.append("./src")

import csv_to_pipe

setup(
    name="csv_to_pipe",
    version=csv_to_pipe.__version__,
    url="https://databricks.com",
    author="david.finch@databricks.com",
    description="wheel file based on csv_to_pipe/src",
    packages=find_packages(where="./src"),
    package_dir={"": "src"},
    entry_points={"packages": ["pipe=csv_to_pipe.convert_csv_to_pipe:csv_to_pipe"]},
    install_requires=[
        # Dependencies in case the output wheel file is used as a library dependency.
        # For defining dependencies, when this package is used in Databricks, see:
        # https://docs.databricks.com/dev-tools/bundles/library-dependencies.html
        "setuptools"
    ],
)
