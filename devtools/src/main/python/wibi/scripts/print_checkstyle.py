#!/usr/bin/env python3
# -*- mode: python -*-
"""Pretty-print checkstyle errors.

Usage:
  From the Maven project directory, run
"""

import os
import sys
import time
import xml.etree.ElementTree as etree


def ListFiles(root, name):
  """Lists all files with the specified name under a given root directory.

  Equivalent of the shell command 'find root -name name'.

  Args:
    root: Base directory to look for the specified file name.
    name: Name of the file to look for.
  Yields:
    All the paths root/**/name of the files with the specified file name.
  """
  path = os.path.join(root, name)
  if os.path.exists(path):
    yield path

  for entry_name in os.listdir(root):
    entry_path = os.path.join(root, entry_name)
    if os.path.isdir(entry_path):
      for match in ListFiles(root=entry_path, name=name):
        yield match


def main(args):
  args = args[1:]
  cwd = os.getcwd()

  if len(args) > 0:
    paths = args
  else:
    paths = ListFiles(root=cwd, name='checkstyle-result.xml')

  for path in paths:
    assert os.path.exists(path)
    xml = etree.parse(path)
    root = xml.getroot()
    for file_node in root.getchildren():
      file_name = file_node.attrib['name']
      file_name = os.path.relpath(file_name, cwd)
      if len(file_node.getchildren()) > 0:
        for error_node in file_node.getchildren():
          print('%-80s\t%s:%s\t%s' % (
              file_name,
              error_node.attrib['line'],
              error_node.attrib.get('column', '?'),
              error_node.attrib['message'],
          ))


if __name__ == '__main__':
  main(sys.argv)
