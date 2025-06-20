from setuptools import find_packages
from setuptools import setup

setup(
    name="rainbowbatch",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "requests",
        "maya",
    ],
)
