import os
import sys

# Add the root directory to the Python path if necessary:
__path = os.path.dirname(os.path.dirname(os.path.realpath(sys.argv[0])))
if __path not in sys.path:
  sys.path.append(__path)
