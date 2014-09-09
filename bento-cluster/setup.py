# !/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- mode: python -*-
import sys

from setuptools import setup


def main(args):
    setup(
        name="kiji-bento-cluster",
        version="2.0.4",

        # Lists the Python modules provided by this package:
        packages=[
            "bento",
        ],

        # Mapping from Python package to source directory in the project:
        package_dir={
            "bento": "src/main/python/bento",
        },

        # Scripts to install in the bin/ folder and made available on the PATH:
        scripts=[
            "src/main/scripts/bento",
            "src/main/scripts/create-hadoop-user",
            "src/main/scripts/update-etc-hosts",
            "src/main/scripts/update-user-hosts",
        ],

        # Dependencies on other Python packages:
        install_requires=[
            "docker-py",
        ],

        # Metadata for upload to PyPI
        author="WibiData",
        author_email="user@kiji.org",
        description="CDH and Datastax Enterprise docker single-node development cluster.",
        license="Apache License 2.0",
        keywords="bento,kijiproject,kiji,hadoop,hbase,cdh,cassandra",
        url="http://www.kiji.org/",
    )


if __name__ == "__main__":
    main(sys.argv)
