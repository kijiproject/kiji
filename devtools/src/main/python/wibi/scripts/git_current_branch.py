#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-

import os
import subprocess
import sys


head_prefix = 'refs/heads/'
remote_prefix = 'refs/remotes/'
tag_prefix = 'refs/tags/'

def FormatRemote(ref):
  if ref.startswith(remote_prefix):
    return ref[len(remote_prefix):]
  elif ref.startswith(head_prefix):
    return ref[len(head_prefix):]
  elif ref.startswith(tag_prefix):
    return ref[len(tag_prefix):]
  else:
    return ref


def FormatRefs(refs):
  symbols = map(str.strip, refs)
  symbols = filter(None, symbols)
  symbols = map(FormatRemote, symbols)
  print(' (%s)' % ' '.join(symbols))


def GitRefs():
  p = subprocess.Popen(
      args=['git', 'show-ref'],
      stdin=None,
      stdout=subprocess.PIPE,
      stderr=subprocess.PIPE
  )
  (out, err) = p.communicate()
  if p.returncode == 0:
    out = out.decode()
    for line in filter(None, out.split('\n')):
      [commit, ref] = line.split()
      yield (commit, ref)
  else:
    return


def Main(args):
  p = subprocess.Popen(
      args=['git', 'log', '--max-count=1', '--format=%H'],
      stdin=None,
      stdout=subprocess.PIPE,
      stderr=subprocess.PIPE
  )
  (out, err) = p.communicate()
  if p.returncode == 0:
    symbols = []
    commit = out = out.decode().strip()
    for commit_hash, ref in GitRefs():
      if commit == commit_hash:
        symbols.append(ref)
    if symbols:
      return FormatRefs(symbols)
    else:
      print(' (c: %s)' % commit)
      return


if __name__ == '__main__':
  Main(sys.argv)
