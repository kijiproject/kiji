#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- mode: python -*-

import os
import subprocess
import sys



def RunPidStat(pid):
  null = open('/dev/null', 'r')
  args=[
    'pidstat',
    '-u',      # report CPU usage
    '-d',      # report disk I/O
    '-p', str(pid), # target this process
    '-h',      # horizontal
    '1',       # poll every second
  ]
  print('Args: %r' % args)
  proc = subprocess.Popen(
    args=args,
    stdin=null,
    stdout=subprocess.PIPE,
    stderr=None,
  )
  null.close()
  return proc


def MonitorProcess(pid):
  nlines = int(os.environ['LINES'])
  proc = RunPidStat(pid)
  headers = None
  counter = 0
  while True:
    line = proc.stdout.readline().decode().strip()

    if len(line) == 0: continue

    if line.startswith('#'):
      if headers is None:
        headers = line[1:]
        print(headers)
      continue

    counter = (counter + 1) % nlines
    if counter == 0: print(headers)
    print(line)
    sys.stdout.flush()


if __name__ == '__main__':
  MonitorProcess(sys.argv[1])
