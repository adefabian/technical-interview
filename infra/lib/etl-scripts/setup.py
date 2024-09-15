"""Setup."""
import os

from setuptools import setup

setup(
    name="shared",
    version="0.1",
    packages=["shared"],
    package_dir={"": f"{os.path.dirname(__file__)}"},
)
