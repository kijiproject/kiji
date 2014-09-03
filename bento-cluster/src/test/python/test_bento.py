#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- mode: python -*-

import os
import sys
import unittest
import uuid
import urllib.error
import urllib.request

import docker
import time

from bento import cluster


GENERATE_BENTO_NAME_TRIES = 100


class TestKijiApi(unittest.TestCase):
    @staticmethod
    def contains_bento_once(bento_name, bentos):
        """Checks to see that there is exactly one bento instance that matches the expected bento
        instance name.

        Args:
            bento_name: The bento instance provided as its string name to check for.
            bentos: The list of bentos to check.
        Returns:
            True if there is exactly one matching bento found.
        """
        matching = [
            matching_system
            for matching_system in bentos
            if matching_system.bento_hostname == bento_name
        ]
        contains_one = len(matching) == 1
        return contains_one

    @staticmethod
    def generate_random_bento_name(bento_system):
        """Generates a random bento instance name.

        Args:
            bento_system: The bento system to use to check for the existence of bento instances.
        Returns:
            A bento name that is not currently in use.
        """
        tries = 0

        bento_name = 'bento-%s' % uuid.uuid4().hex
        all_bentos = bento_system.list_bentos(only_running=False)
        while TestKijiApi.contains_bento_once(bento_name, all_bentos) \
                and tries < GENERATE_BENTO_NAME_TRIES:
            bento_name = 'bento-%s' % uuid.uuid4().hex
            tries += 1
        return bento_name

    def test_bento_system(self):
        """Tests the bento.cluster.BentoSystem API."""
        bento_system = cluster.BentoSystem(docker_client=docker.Client())
        bento_name = self.generate_random_bento_name(bento_system)

        bento_system.pull_bento()
        test_bento = bento_system.create_bento(bento_name)
        self.assertTrue(
            expr=self.contains_bento_once(bento_name, bento_system.list_bentos(only_running=False)),
            msg='Failed to find created bento: %s' % bento_name,
        )

        # Test bento itself.
        test_bento.start()
        self.assertTrue(
            expr=self.contains_bento_once(bento_name, bento_system.list_bentos()),
            msg='Failed to find started bento: %s' % bento_name,
        )
        self.assertTrue(
            expr=test_bento.is_container_running,
            msg='Failed to start bento instance: %s' % bento_name
        )
        self.assertTrue(
            expr=test_bento.is_running,
            msg='Failed to start bento instance: %s' % bento_name
        )
        self.assertEqual(test_bento.bento_hostname, bento_name)

        # Check that the web uis are accessible via ip.
        self.assertEqual(
            first=200,
            second=urllib.request.urlopen('http://%s:8088/cluster' % test_bento.bento_ip).getcode(),
            msg='Yarn ResourceManager Web UI not responding.',
        )
        self.assertEqual(
            first=200,
            second=urllib.request.urlopen('http://%s:8888/' % test_bento.bento_ip).getcode(),
            msg='DataStax Web UI not responding.',
        )
        self.assertEqual(
            first=200,
            second=urllib.request.urlopen('http://%s:9001/' % test_bento.bento_ip).getcode(),
            msg='Supervisord Web UI not responding.',
        )
        self.assertEqual(
            first=200,
            second=urllib.request.urlopen('http://%s:50070/' % test_bento.bento_ip).getcode(),
            msg='HDFS NameNode Web UI not responding.',
        )
        self.assertEqual(
            first=200,
            second=urllib.request.urlopen('http://%s:60010/' % test_bento.bento_ip).getcode(),
            msg='HBase Master Web UI not responding.',
        )

        test_bento.stop()
        self.assertTrue(
            expr=(not test_bento.is_container_running),
            msg='Failed to start bento instance: %s' % bento_name
        )
        self.assertTrue(
            expr=(not test_bento.is_running),
            msg='Failed to start bento instance: %s' % bento_name
        )
        self.assertTrue(
            expr=(not self.contains_bento_once(bento_name, bento_system.list_bentos())),
            msg='Failed to stop bento: %s' % bento_name,
        )

        bento_system.delete_bento(bento_name)
        self.assertTrue(
            expr=(not self.contains_bento_once(bento_name, bento_system.list_bentos(False))),
            msg='Failed to delete bento: %s' % bento_name,
        )

# --------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    print('Not a standalone module: %r. Use: "test_runner.py <module>" instead.' % __file__)
    sys.exit(os.EX_USAGE)