#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- mode: python -*-

"""Lists the server processes running locally on the machine.

To get the details for all the servers, run as root, eg. with sudo.
"""

import os
import re
import subprocess
import sys


RE_HOST_PORT = re.compile(r'(?P<host>.*):(?P<port>[0-9]*)')



def JavaProcesses():
  """Lists the Java processes.

  Returns:
    A map from PID to Java process name.
  """
  process = subprocess.Popen(
    args=['jps'],
    stdin=None,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE
  )
  (out, err) = process.communicate()
  assert (process.returncode == 0), err

  java_map = {}

  lines = out.decode().split('\n')
  lines = filter(None, lines)
  for line in lines:
    (pid, name) = line.split()
    pid = int(pid)
    java_map[pid] = name

  return java_map


def Main(args):
  process = subprocess.Popen(
    args=[
      '/bin/netstat',
      '--listening',
      '--tcp',
      '--numeric',
      '--program',
      '--extend',
    ],
    stdin=None,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE
  )
  (out, err) = process.communicate()
  assert (process.returncode == 0), err

  # Map: PID -> (program name, collection of addresses)
  server_map = {}

  lines = out.decode().split('\n')
  lines = filter(None, lines[2:])
  for line in lines:
    fields = line.split()

    pid_program = fields[8]
    if pid_program == '-':
      pid = -1
      program = None
    else:
      (pid, program) = pid_program.split('/')
      pid = int(pid)

    hpmatch = RE_HOST_PORT.match(fields[3])
    host = hpmatch.group('host')
    port = hpmatch.group('port')
    if host in ('0.0.0.0', '::'):
      host = '*'
    elif host in ('127.0.0.1', '::1'):
      host = 'local'

    entry = server_map.get(pid)
    if entry is None:
      entry = (program, set())
      server_map[pid] = entry

    entry[1].add('%s:%s' % (host, port))

  # Lazily initialized map: (PID -> Java program name):
  java_map = None

  print('%5s %-60s %s' % ('PID', 'Program name', 'Server ports'))

  for pid, (program, addrs) in sorted(server_map.items()):
    if pid == -1:
      pid = 'N/A'
    if program == 'java':
      if java_map is None:
        java_map = JavaProcesses()
      program = 'java(%s)' % java_map[pid]
    if program is None:
      program = 'N/A'

    print('%5s %-60s %s' % (pid, program, ' '.join(sorted(addrs))))


if __name__ == '__main__':
  Main(sys.argv)
