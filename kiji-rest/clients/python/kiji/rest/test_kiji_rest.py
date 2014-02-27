#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- mode: python -*-

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

"""Unit-tests for the KijiREST client."""

import logging
import os
import sys
import unittest

from base import base

from kiji.rest import kiji_rest


class TestKijiRest(unittest.TestCase):

  def testRestEntityId(self):
    """Tests the entity ID normalization."""
    self.assertEqual([1], kiji_rest._RestEntityId(1))
    self.assertEqual([1], kiji_rest._RestEntityId([1]))
    self.assertEqual([1, 2], kiji_rest._RestEntityId([1, 2]))
    self.assertEqual(['1'], kiji_rest._RestEntityId('1'))
    self.assertEqual(['1'], kiji_rest._RestEntityId(['1']))
    self.assertEqual(['1', '2'], kiji_rest._RestEntityId(['1', '2']))
    self.assertEqual(('1', '2'), kiji_rest._RestEntityId(('1', '2')))


def Main(args):
  args = list(args)
  args.insert(0, sys.argv[0])
  unittest.main(argv=args)


if __name__ == '__main__':
  base.Run(Main)
