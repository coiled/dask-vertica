#!/usr/bin/env python

from setuptools import setup

setup(
    name="dask-vertica",
    version="0.0.1",
    description="Dask + Veertica intergration",
    license="BSD",
    maintainer="Paul Hobson",
    maintainer_email="paul@coiled.io",
    packages=["dask_vertica"],
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    python_requires=">=3.8",
    install_requires=open("requirements.txt").read().strip().split("\n"),
    include_package_data=True,
    zip_safe=False,
)
