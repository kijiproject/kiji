#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import glob
import os
import sys

import setuptools
from setuptools.command import test


class TestWithDiscovery(test.test):
    def finalize_options(self):
        super().finalize_options()
        self.test_args.insert(0, 'discover')
        # if tests are in same namespace as module, make sure
        # test_dir is on sys.path before setup loads module (before test discovery)
        test_path = os.path.abspath(self.test_suite)
        if os.path.exists(test_path):
            sys.path.insert(0, test_path)


SRC_PATH = os.path.join("src", "main", "python")
TEST_PATH = os.path.join("src", "test", "python")


def list_scripts():
    return glob.glob("scripts/*")


def get_version_string():
    root_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    version_path = os.path.join(root_dir, SRC_PATH, "base", "VERSION")
    with open(version_path, mode="rt", encoding="UTF-8") as file:
        return file.read().strip()


def main():
    assert (sys.version_info[0] >= 3 and sys.version_info[1] >= 4), \
        "Python version >= 3.4 required, got %r" % (sys.version_info,)

    version = get_version_string()

    setuptools.setup(
        name='python-base',
        version=version,
        packages=setuptools.find_packages(SRC_PATH),
        namespace_packages=setuptools.find_packages(SRC_PATH),
        package_dir={
            '': SRC_PATH,
        },
        scripts=sorted(list_scripts()),
        package_data={
            "base": ["VERSION"],
        },
        test_suite=TEST_PATH,
        cmdclass={'test': TestWithDiscovery},

        # metadata for upload to PyPI
        author="WibiData engineers",
        author_email="eng@wibidata.com",
        description='General purpose library for Python.',
        license='Apache License 2.0',
        keywords='python base flags tools workflow',
        url="http://wibidata.com",
    )


if __name__ == '__main__':
    main()
