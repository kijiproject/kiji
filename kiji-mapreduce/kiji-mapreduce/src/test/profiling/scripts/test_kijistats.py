#!/usr/bin/python
# Copyright 2013 WibiData, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''
test_kijistats.py is a script to unit test the kijistats.py script to ensure it is
performing the expected computations, such as aggregating based on jobname or function name.
It uses pre-existing files in kiji-mapreduce/src/test/profiling/resources/ that
contain some sample output that was collected from some profiled MapReduce jobs.

The file name of the resource files represents a MapReduce task attempt id.

The format of the resources files is as follows:
Job Name, Job ID, Task Attempt, Function Signature, Aggregate Time (nanoseconds), Number of Invocations, Time per call (nanoseconds)

From *this* directory, you can run the test script as:
./test_kijistats.py

On MacOSX, you may need to run this as:
arch -i386 /usr/bin/python test_kijistats.py

'''

import os
import sys
import unittest
# path to where the kijistats script is located
srcpath = os.path.abspath(os.getcwd() + '/../../../main/profiling/scripts')
sys.path.insert(0, srcpath)
import kijistats
import tempfile

# Unit test for the kijistats.py script
class TestKijiStats(unittest.TestCase):
    def setUp(self):
        # The directory in which the resource files containing sample profiling data reside.
        self.input_dir = os.path.abspath(os.getcwd() + '/../resources')

    def testArgsForJob(self):
        # Raise error since we must specify either job id, job name, task attempt or function
        # for which you wish to collect stats.
        self.assertRaises(ValueError, kijistats.main, ['kijistats', '--to-file', 'xyz'])

    # Aggregate profiling stats according to Job Name. E.g. You may like to see if the stats are
    # consistent across all runs of a particular Gatherer, in this case, TestingGatherer.
    def testByJobName(self):
        expectedOutput = "public synchronized org.kiji.schema.KijiSchemaTable.SchemaEntry " + \
            "org.kiji.schema.impl.HBaseSchemaTable.getSchemaEntry(org.kiji.schema.util.BytesKey)," + \
            " 278514701, 875, 709618.05"
        tf = tempfile.NamedTemporaryFile(delete=False)
        filename = tf.name
        kijistats.main(['./kijistats.py',
                        '--stats-dir',
                        self.input_dir,
                        '--by-jobname',
                        'TestingGatherer',
                        '--to-file',
                        filename])
        with open(filename) as f:
            for line in f:
                if "getSchemaEntry" in line:
                    self.assertEqual(expectedOutput, line.rstrip())
                    return
        self.fail("Expected line not found in output")

    # Aggregate profiling stats across all calls to a certain function. E.g. You may want to ensure
    # that avro counter encoder behaves consistently across all calls to it.
    def testByFunction(self):
        expectedOutput = "attempt_local1340085606_0004_m_000000_0, 157036000, 320, 490737.5"
        tf = tempfile.NamedTemporaryFile(delete=False)
        filename = tf.name
        kijistats.main(['./kijistats',
                        '--stats-dir',
                        self.input_dir,
                        '--by-function',
                        'getSchemaEntry',
                        '--to-file',
                        filename])
        with open(filename) as f:
            for line in f:
                if "attempt_local1340085606_0004_m_000000_0" in line:
                    self.assertEqual(expectedOutput, line.rstrip())
                    return
        self.fail("Expected line not found in output")

    # Aggregate profiling stats across all attempts of a particular job. Instead of per attempt, you
    # may wish to see how a function performs across all attempts to get an average.
    def testByJobID(self):
        expectedOutput = "public synchronized org.kiji.schema.KijiSchemaTable.SchemaEntry " + \
            "org.kiji.schema.impl.HBaseSchemaTable.getSchemaEntry(org.kiji.schema.util.BytesKey)," + \
            " 157036000, 320, 490737.5"
        tf = tempfile.NamedTemporaryFile(delete=False)
        filename = tf.name
        kijistats.main(['./kijistats',
                        '--stats-dir',
                        self.input_dir,
                        '--by-job',
                        'job_local1340085606_0004',
                        '--to-file',
                        filename])
        with open(filename) as f:
            for line in f:
                if "getSchemaEntry" in line:
                    self.assertEqual(expectedOutput, line.rstrip())
                    return
        self.fail("Expected line not found in output")

suite = unittest.TestLoader().loadTestsFromTestCase(TestKijiStats)
unittest.TextTestRunner(verbosity=2).run(suite)
