#!/usr/bin/env python3
#-*- coding: utf-8 -*-
#-*- mode: python -*-
import sys

from setuptools import setup


def main(args):
    setup(
        name="kiji-bento-cluster",
        version="2.0.10",

        # Lists the Python modules provided by this package:
        packages=[
            "bento",
        ],

        # Mapping from Python package to source directory in the project:
        package_dir={
            "bento": "src/main/python/bento",
        },

        # Templates to include.
        package_data={
            "bento": [
                "bento-env-template.sh",
                "bento-sudoers-template",
                "core-site-template.xml",
                "hdfs-site-template.xml",
                "yarn-site-template.xml",
                "mapred-site-template.xml",
                "hbase-site-template.xml",
            ],
        },

        # Scripts to install in the bin/ folder and made available on the PATH:
        scripts=[
            "src/main/scripts/bento",
            "src/main/scripts/create-hadoop-user",
            "src/main/scripts/update-etc-hosts",
            "src/main/scripts/update-user-hosts",
            "src/main/scripts/add-bento-route",
        ],

        # Dependencies on other Python packages:
        install_requires=[
            "docker-py==0.6.0",
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
