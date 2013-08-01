#!/usr/bin/python
# Copyright 2013 WibiData, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''
    kijistats aggregates the data collected by profiling KijiSchema and
    KijiMR during a MapReduce job. The KijiMR-profiling jar generates
    a file for every task attempt, whose format is as follows:
    "Job Name, Job ID, Task Attempt, Function Signature,
    Aggregate Time (nanoseconds), Number of Invocations,
    Time per call (nanoseconds)\n"
    A user may want to aggregate these numbers based on JobId to get a
    picture at job-level instead of task attempt granularity.
    The stats folder is under the job's working directory in HDFS. You will
    need to copy this to your local filesystem before passing it as input
    to this command.

    This should be run as:
    ./kijistats.py [..arguments]

    On MacOSX, you may need to run the script as
    arch -i386 /usr/bin/python ./kijistats.py [..arguments]

    Use the -h option to see the various ways in which the profiling stats
    can be aggregated.
    The output can be stored as a csv file or displayed as bar graphs of
    aggregate times spent in functions, number of invocations of functions
    and average time per function call for functions. This will be per task
    attempt or per function based on context.

    NOTE: you may need to install some python libraries if they do not exist
    on your system. (You can use either pip or easy_install for this.)

    pip install matplotlib
    pip install numpy

'''

import argparse
from collections import defaultdict
import matplotlib.pyplot as plt
import os
import re
import sys

"""
This script parses comma delimited files. The various fields are defined positionally below.
"""
JOB_NAME = 0
JOB_ID = 1
TASK_ATTEMPT = 2
FUNC_SIG = 3
AGGREGATE_TIME = 4
INVOCATIONS = 5
PER_CALL_TIME = 6

"""
Graph colors
"""
# Color for aggregate time spent in function in the output graph
COLOR_AGGR = '#cc99ff'
# Color of number of invocations of function in the output graph
COLOR_INV = '#ff99ff'
# Color of average time spent per function call in the output graph
COLOR_PCT = '#cc0099'


def BuildFlagParser():
    """Flag parsers for the Kiji stats tool.

    Returns:
    Command-line flag parser.
    """
    parser = argparse.ArgumentParser(
        description='Kiji Stats tool to analyze profiling data.'
    )
    parser.add_argument(
        '--stats-dir',
        dest='stats_dir',
        type=str,
        default=os.getcwd(),
        help='Local directory where profiling data is stored. ' +
        'Usually called `kijistats` in the working directory of the MapReduce job. ' +
        'You will need to copy this directory on hdfs to your local filesystem and supply it.'
    )
    jobgrp = parser.add_mutually_exclusive_group(required=False)
    jobgrp.add_argument(
        '--by-job',
        dest='job',
        type=str,
        help='Job ID for which to collect stats.'
    )
    jobgrp.add_argument(
        '--by-jobname',
        dest='jobname',
        type=str,
        help='Name of the job for which to collect stats. This will be ignored if' +
        ' the job ID has been specified.' +
        ' Note that multiple jobs may have the same name. Example: MapFamilyGatherer.'
    )
    jobgrp.add_argument(
        '--by-task',
        dest='taskid',
        type=str,
        help='Task attempt ID for which to collect stats.'
    )
    parser.add_argument(
       '--by-function',
       dest='function_name',
       type=str,
       help='Function for which to collect stats. Can be used to collect stats about' +
       ' this function across all task attempts or jobs or for a single task or job.'
    )
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument('--as-graph', action='store_true')
    grp.add_argument('--to-file', type=str,
                     help='File name to store aggregated result.')
    return parser

'''
This function filters the input files based on the options specified by the user.
The user may aggregate by jobID, job name, task ID, function name or a combination
where applicable. (Use kijistats -h for more information.). Before we aggregate the
numbers, we would like only the relevant data to be present.
The output is a list of select lines from the input file that match the filtering
criteria, but have now been split on ", "
E.g.
[
[job id, job name ...]
[job id, job name...]
]
You can use the above defined positional constants like JOB_NAME to index into each
line.
'''
def gatherData(flags):
    stats_dir = flags.stats_dir
    taskid = flags.taskid
    jobid = flags.job
    jobname = flags.jobname
    function_name = flags.function_name
    profile_data = []
    if not taskid:
        # Aggregate all files under the stats directory
        for filename in os.listdir(stats_dir):
            filename = os.path.join(stats_dir, filename)
            if os.path.isfile(filename) and not filename.startswith(".") \
                and not filename.startswith("_") and not filename.endswith("crc"):
                with open(filename) as f:
                    for line in f.readlines():
                        if line.startswith('Job Name, Job ID,'):
                            continue
                        # We split first so that we dont accidentally match strings like
                        # "FooTestingGatherer" to "TestingGatherer" while filtering.
                        # We need regular expressions because otherwise we might
                        # split on commas in function signatures
                        # This regular expression splits on comma only if the comma is not
                        # present in the middle of parentheses. The regex matches:
                        # - non-comma, non-open-paren characters
                        # - strings that start with an open paren, contain 0 or more
                        # non-close-parens, and then a close paren
                        r = re.compile(r'(?:[^,(]|\([^)]*\))+')
                        splitline = [x.strip() for x in r.findall(line)]
                        if len(splitline) > PER_CALL_TIME + 1:
                            raise RuntimeError('Possible error in input format. More than 6 +'
                            'elements found in line', line)
                        # Filtering on job id
                        if jobid:
                            if  jobid == splitline[JOB_ID]:
                                # Filtering on function within jobid
                                if (function_name and function_name in splitline[FUNC_SIG]) or \
                                    not function_name:
                                    profile_data.append(splitline)
                        # Filtering on job name (need not be perfect match)
                        elif jobname:
                            if jobname in splitline[JOB_NAME]:
                                # Filtering on function within job name
                                if (function_name and function_name in splitline[FUNC_SIG]) or \
                                    not function_name:
                                    profile_data.append(splitline)
                        # Filtering on function name across all jobs
                        elif function_name and function_name in splitline[FUNC_SIG]:
                            profile_data.append(splitline)
    else:
        # We only need to read the file which represents this task attempt
        if os.path.exists(os.path.join(stats_dir, taskid)):
            with (open(os.path.join(stats_dir, taskid))) as f:
                for line in f.readlines():
                    # disregard the header line
                    if line.startswith('Job Name, Job ID,'):
                        continue
                    # Space after ',' is important because that is how profile data is
                    # formatted. We split first so that we dont accidentally match strings
                    # "FooTestingGatherer" to "TestingGatherer" while filtering.
                    # We need regular expressions because otherwise we might
                    # split on commas in function signatures
                    r = re.compile(r'(?:[^,(]|\([^)]*\))+')
                    splitline = [x.strip() for x in r.findall(line)]
                    if len(splitline) > PER_CALL_TIME + 1:
                        raise RuntimeError('Possible error in input format. More than 6 +'
                        'elements found in line', line)
                    # Filtering on function within jobid
                    if (function_name and function_name in splitline[FUNC_SIG]) or \
                        not function_name:
                        profile_data.append(splitline)
    return profile_data

'''
This plots 3 graphs for: aggregate time in a function, number of invocations of a function and
average time per call spent in a function. All the times are in nanoseconds.
If we are aggregating by function name, the graph is displayed by task attempt id. In all other
cases, the numbers are displayed by function name.
'''
def plotgraph(data):
    N = len(data)
    labels =[]
    aggr = []
    inv = []
    pct = []
    for key, value in data.items():
        labels.append(key)
        aggr.append(value[0])
        inv.append(value[1])
        pct.append(value[2])

    ind = range(N)  # the x locations for the groups
    height = 0.7       # the width of the bars
    plt.figure()
    plt.barh(ind, aggr, align='center', height=height, color=COLOR_AGGR)
    plt.yticks(ind, labels, horizontalalignment='left')
    plt.title('Aggregate time in nanoseconds')

    plt.figure()
    plt.barh(ind, inv, align='center', height=height, color=COLOR_INV)
    plt.yticks(ind, labels, horizontalalignment='left')
    plt.title('Number of invocations')

    plt.figure()
    plt.barh(ind, pct, align='center', height=height, color=COLOR_PCT)
    plt.yticks(ind, labels, horizontalalignment='left')
    plt.title('Per call time')
    plt.show()

'''
Combine the data based on context. By this point, raw_data contains the profiling stats
either by function signature or by task attempt id, depending on what the user specified.
Add up the numbers to present an aggregate view.
The input raw_data  is a list of select lines from the input file that match the filtering
criteria, but have now been split on ", "
E.g.
[
[job id, job name ...]
[job id, job name...]
]
You can use the above defined positional constants like JOB_NAME to index into each
line.
The output is a dictionary from (function name | attempt id) depending on context to a
tuple of (aggregate time, number of invocations, average time we call). Times are all
in nanoseconds.
'''
def aggregateData(flags, raw_data):
    taskid = flags.taskid
    jobid = flags.job
    jobname = flags.jobname
    function_name = flags.function_name
    aggregated = {}
    if jobid or jobname or taskid:
        # We have either accumulated one (or all functions) from all task attempts
        # Combine these by function name
        d_aggr = defaultdict(int)
        d_inv = defaultdict(int)
        d_pct = defaultdict(float)
        for row in raw_data:
            d_aggr[row[FUNC_SIG]] += int(row[AGGREGATE_TIME])
            d_inv[row[FUNC_SIG]] += int(row[INVOCATIONS])
            d_pct[row[FUNC_SIG]] += float(row[PER_CALL_TIME])
        for key in d_aggr.keys():
            aggregated[key] = (d_aggr[key], d_inv[key], d_pct[key])
    elif function_name:
        # At this point, we are trying to view this function across task attempts
        d_aggr = defaultdict(int)
        d_inv = defaultdict(int)
        d_pct = defaultdict(float)
        for row in raw_data:
            d_aggr[row[TASK_ATTEMPT]] += int(row[AGGREGATE_TIME])
            d_inv[row[TASK_ATTEMPT]] += int(row[INVOCATIONS])
            d_pct[row[TASK_ATTEMPT]] += float(row[PER_CALL_TIME])
            for key in d_aggr.keys():
                aggregated[key] = (d_aggr[key], d_inv[key], d_pct[key])
    return aggregated

'''
Either plot the graphs described above in plotgraph or serialize to a file. The format is
a comma separated list of (function signature | task attempt id), aggregate time spent in function
(nanoseconds), number of invocations, average time spent per function (nanoseconds)'\n'
'''
def displayData(flags, aggregated):
    if flags.to_file:
        with open(flags.to_file, 'w') as f:
            for key in aggregated:
                f.write(key + ', ' + ', '.join([str(x) for x in aggregated[key]]) + '\n')
    else:
        plotgraph(aggregated)


def main(args):
    FLAGS = BuildFlagParser().parse_args(args[1:])
    FLAGS.stats_dir = os.path.abspath(os.path.expanduser(FLAGS.stats_dir))
    if not (FLAGS.job or FLAGS.jobname or FLAGS.taskid or FLAGS.function_name):
        raise ValueError('Incorrect Arguments: You must specify either job id, job name, '
                         'task attempt or function for which you wish to collect stats.')
    raw_data = gatherData(FLAGS)
    aggregated = aggregateData(FLAGS, raw_data)
    displayData(FLAGS, aggregated)

if __name__ == '__main__':
    main(sys.argv)
