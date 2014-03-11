#!/usr/bin/env python3
#
#   (c) Copyright 2014 WibiData, Inc.
#
#   See the NOTICE file distributed with this work for additional
#   information regarding copyright ownership.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
#
#   The express script provides tools for running Express jobs and
#   interacting with the KijiExpress system.
#   Tools are run as:
#
#   bash> $EXPRESS_HOME/bin/express.py <command-name> [--flags] args...
#
#   For full usage information, use:
#
#   bash> $EXPRESS_HOME/bin/express.py help
#


import logging
import os
import subprocess

from base import base
from base import cli
from base import command

FLAGS = base.FLAGS
LogLevel = base.LogLevel
Default = base.Default

KIJI_EXPRESS_TOOL = 'org.kiji.express.flow.ExpressTool'


# ------------------------------------------------------------------------------


class Command(cli.Action):
  """CLI action with express script specifics."""

  def __init__(self):
    """Initializes a CLI action and add environment as property.

    """
    super(Command, self).__init__()
    self._env = dict(os.environ)

  @property
  def env(self):
    """Returns: Dictionary representing the environment."""
    return self._env

  def add_to_list(self,x, y):
    """ Add a list of strings to a list, if you add a list of a single string,
        extend will break up the string into its characters.

    Args:
      x: list to append to.
      y: list to add.
    Returns:
      The list that looks like x::y
    """
    if len(y) > 1:
      x.extend(y)
    elif len(y) == 1:
      x.append(y[0])
    return x

  def add_string(self, l, string, delimeter):
    """ Add a string of elements separated by a delimeter to a list.

    Args:
      l: list to append to.
      string: string
      delimeter: delimeter to split the string up by.
    Returns:
      The string split up by the delimeter concatenated onto the list l.
    """
    assert(type(string) == str)
    assert(type(l) == list)
    new_list = l
    if string != '':
      new_list = self.add_to_list(l, string.split(delimeter))
    return new_list


  def check_env(self):
    """ Checks that all env vars used by script are set. Raises exception if one
        of them is not found.

    Args:
      env: A dictionary mapping environment variables name to values.
    Returns:
      Nothing.
    """
    keys = ['EXPRESS_HOME', 'HBASE_HOME', 'HADOOP_HOME', 'KIJI_HOME',
        'KIJI_MR_HOME', 'SCHEMA_SHELL_HOME']
    for key in keys:
      if (self.env.get(key) == None):
        raise Exception('Please set your %s environment variable.' % (key))

  def hadoop_ver(self, env, hadoop_ver):
    """ Uses hadoop binary to find the hadoop version if user does not specify
        their own.

    Args:
      env: A dictionary mapping environment variables name to values.
      hadoop_ver: Manually overrides the hadoop binary setting the version.
    Returns:
      The hadoop major version to use, e.g. 'hadoop2'.
    """
    hadoop_mjr_ver = hadoop_ver
    if hadoop_mjr_ver == "":
      path = os.path.join(env.get('HADOOP_HOME'), 'bin/hadoop')
      cmd = [path, 'version']
      version_str = subprocess.check_output(cmd,  universal_newlines=True)
      hadoop_mjr_ver = 'hadoop' + version_str.split()[1][0]
      logging.debug('No hadoop version sepcified by a flag. Detected \'%s\' as '
          'hadoop version.' % (hadoop_mjr_ver))
    return hadoop_mjr_ver

  def jars_dir(self, env, key):
    """ Make the jars for a project available by appending their lib/*
        directory to the classpath.

    Args:
      env: A dictionary mapping environment variables name to values.
      key: The env dictionary key to return jars for. Env var should point to
          the root dir for a project (e.g. env['SCHEMA_HOME']=path/to/schema)

    Returns:
      A string for the directory containing the jars for the given project.
    """
    path = os.path.join(os.path.abspath(env.get(key)), 'lib/*')
    logging.debug('The jars for the project %s are located in %s.' %
      (key, path))
    return path

  def dist_specific_jars(self, env, key, hadoop_ver):
    """Schema and MR need jars specific to the version of Hadoop, this function
      finds those.

    Args:
      env: A dictionary mapping environment variables name to values.
      key: The env dictionary key to return jars for.
      hadoop_ver: String representing hadoop distribution version, e.g. hadoop2.
    Returns:
      A string for the directory containing the dist specific jars for the given
      project.
    """
    dir_path = os.path.join(
        os.path.abspath(env.get(key)),
        'lib/distribution',
        '%s/*' % (hadoop_ver))
    logging.debug('The distribution specific jars for the project %s are '
      'located in %s.' % (key, dir_path))
    return dir_path

  def lib_jar_classpath(self, env, hadoop_ver):
    """ Finds all the jars from the various kiji projects express requires.

    Args:
      env: A dictionary mapping environment variables name to values.
      hadoop_ver: String representing hadoop distribution version, e.g. hadoop2.
    Returns:
      Lib jars from the source folders of the following projects in a list:
      express, mr dist specific jars, mr, schema dist specific jars, schema,
      schema shell, and modeling.
    """
    cp = []
    cp.append(self.jars_dir(env, 'EXPRESS_HOME'))
    if (env.get('KIJI_MR_HOME') != env.get('KIJI_HOME')):
      cp.append(self.dist_specific_jars(env, 'KIJI_MR_HOME', hadoop_ver))
      cp.append(self.jars_dir(env, 'KIJI_MR_HOME'))
    cp.append(self.dist_specific_jars(env, 'KIJI_HOME', hadoop_ver))
    cp.append(self.jars_dir(env, 'KIJI_HOME'))
    cp.append(self.jars_dir(env, 'SCHEMA_SHELL_HOME'))
    if env.get('MODELING_HOME') != None:
      cp.append(self.jars_dir(env, 'MODELING_HOME'))
    else:
      logging.warning('MODELING_HOME environment variable not set.')
    logging.debug('This is the libjars_cp:\n %s' % (cp))
    return cp

  def classpath_from_executable(self, env, key, binary_name):
    """ Used to get output of "${ROOT_DIR}/bin/${executable} classpath"

    Args:
      env: Dictionary representing the environment and its variables.
      key: The env dictionary key holding the ${ROOT_DIR} above.
    Returns:
      A list of strings, representing the classpath of jars for the binary.
    """
    bin_path = os.path.join(env.get(key), 'bin', binary_name)
    cmd = [bin_path, 'classpath']
    cp_str = subprocess.check_output(cmd,  universal_newlines=True)
    cp_str_sans_endline = cp_str.replace('\n', '')
    cp_str_sans_slf4j = cp_str_sans_endline.replace('slf4j', '')
    cp = cp_str_sans_slf4j.split(':')
    logging.debug('The classpath provided by the binary %s is:\n%s' % (
        bin_path, '\n'.join(cp)))
    return cp

  def create_classpath(self, env, hadoop_ver):
    """
    Args:
      env: A dictionary mapping environment variables name to values.
      hadoop_ver: String representing hadoop distribution version, e.g. hadoop2.
    Returns:
      List of Strings to be used as class path with java command.
    """
    cp = []
    # prepend the user's jars along with value of $KIJI_CLASSPATH
    cp = self.add_string(cp, self.flags.libjars, ',')
    cp = self.add_string(cp, env.get('KIJI_CLASSPATH', ''),':')
    cp.append(os.path.join(os.path.abspath(env.get('EXPRESS_HOME')), 'conf'))
    cp.extend(self.lib_jar_classpath(env, hadoop_ver))
    hadoop_jars = self.classpath_from_executable(env, 'HADOOP_HOME', 'hadoop')
    hbase_jars = self.classpath_from_executable(env, 'HBASE_HOME', 'hbase')
    cp.extend(hadoop_jars)
    cp.extend(hbase_jars)
    return cp

  def tmpjar_classpath(self, env, hadoop_ver):
    """ Use TmpJarsTool to prepare jars to be sent to the distributed cache.

    Args:
      env: A dictionary mapping environment variables name to values.
      hadoop_ver: String of hadoop distribution version, e.g. 'hadoop2'.
    Returns:
      Comma separated string of jars to be sent to distributed cache for use
      locally on nodes.
    """
    tmpjars_cp=self.flags.libjars.split(',')
    tmpjars_cp.extend(self.lib_jar_classpath(env, hadoop_ver))
    express_cp = ":".join(self.create_classpath(env, hadoop_ver))
    cmd = ['java',
        '-cp',
        express_cp,
        'org.kiji.express.tool.TmpJarsTool',
        ':'.join(tmpjars_cp)]
    output = subprocess.check_output(cmd)
    tmpjars = output.decode().strip()
    logging.debug('The generated temp jars classpath, which will be sent to the'
        'distributed cache, is:\n%s' % (tmpjars.split(',')))
    return tmpjars

class Classpath(Command):
  USAGE = """
    |
    |Prints the classpath used for express script related commands.
    |
    |Usage:
    |  %(this)s --libjars=path/to/jar1,path/to/jar2 --hadoop_ver=hadoop1|hadoop2
  """

  def RegisterFlags(self):
    self.flags.AddString(
        name='hadoop_ver',
        default="",
        help=('Either \'hadoop1\' or \'hadoop2\', use this to manually set the'
            'version of hadoop. If unset, script will resolve version by'
            'querying binary in $HADOOP_HOME directory.'),
    )
    self.flags.AddString(
        name='libjars',
        default="",
        help=('Comma separated list of third party jars to append/prepend to'
            'the java classpath. These take precedence over the jars from'
            'KIJI_CLASSPATH env var. Can be a directory path that holds jars.'),
    )

  def Run(self, args=''):
    self.check_env()
    hadoop_ver = self.hadoop_ver(self.env, self.flags.hadoop_ver)
    cp = self.create_classpath(self.env, hadoop_ver)
    log_list = list(map(lambda path: os.path.abspath(path), cp))
    print(':'.join(log_list))

class Jar(Command):
  USAGE = """
    |
    |Runs an arbitrary Scala or Java program, utilizing express classpath.
    |
    |Usage:
    |  %(this)s
    |    --libjars=path/to/jar1,path/to/jar2
    |    --user_jar=org.MyBigDataApp.jar
    |    --class_name=org.MyBigDataApp.MyJob
    |    --hadoop_ver=hadoop1|hadoop2
    |    --mode=local|hdfs
    |    arguments...
  """

  def RegisterFlags(self):
    self.flags.AddString(
        name='libjars',
        default="",
        help=('Comma separated list of third party jars to append/prepend to'
            'the java classpath. These take precedence over the jars from'
            'KIJI_CLASSPATH env var. Can be a directory path that holds jars.'),
    )
    self.flags.AddString(
        name='user_jar',
        default="",
        help=('Jar that the user supplies to express command that contains the'
            'job they hope to run.'),
    )
    self.flags.AddString(
        name='class_name',
        default="",
        help=('Java class name to run with the express classpath.')
    )
    self.flags.AddString(
        name='hadoop_ver',
        default="",
        help=('Either \'hadoop1\' or \'hadoop2\', use this to manually set the'
            'version of hadoop. If unset, script will resolve version by'
            'querying binary in $HADOOP_HOME directory.'),
    )
    self.flags.AddString(
        name='mode',
        default='local',
        help=('Hadoop mode to run in. Must be hdfs or local..'),
    )

  def java_cmd(
      self,
      express_cp,
      java_opts,
      class_name,
      user_args,
  ):
    """ Create a list to pass to python.subprocess to launch jar.

    Args:
      express_cp: Classpath of kiji project jars, hadoop, and hbase.
      java_opts: Java options to configure the jvm.
      class_name: The class user wants to run.
      args: A list of unparsed arguments to get sent to jar.
    """
    assert(type(express_cp) == str)
    assert(type(java_opts) == str)
    assert(type(class_name) == str)
    assert(type(user_args) == list)
    # make sure nothing is an empty string, necessary check for java_opts
    cmd = ['java', '-cp']
    #don't add any empty strings to the command list
    f = lambda x: cmd.append(x) if len(x) > 0 else cmd
    # the following property only needed in kiji-schema v1.1
    prop = '-Dorg.kiji.schema.impl.AvroCellEncoder.SCHEMA_VALIDATION=DISABLED'
    for x in [express_cp, java_opts, prop, class_name]:
      f(x)
    if len(user_args) == 1:
      cmd.append(user_args[0])
    elif len(user_args) > 1:
      cmd.extend(user_args)
    return cmd


  def Run(self, args=''):
    self.check_env()
    logging.debug('Running the jar command.')
    if (self.flags.user_jar == ''):
      raise Exception('Please specify a  user_jar flag.')
    if (self.flags.class_name == ''):
      raise Exception('Please specify a  class_name flag.')
    cp = [self.flags.user_jar]
    hadoop_ver = self.hadoop_ver(self.env, self.flags.hadoop_ver)
    express_cp = self.create_classpath(self.env, hadoop_ver)
    cp.extend(express_cp)
    cmd = self.java_cmd(
      express_cp=express_cp,
      user_jar=user_jar,
      class_name=self.flags.class_name,
      user_args=list(args),
    )
    logging.info('Trying to execute command:\n%s' % ('\n'.join(cmd)))
    subprocess.call(cmd)

class Job(Command):
  USAGE = """
    |
    |Runs a compiled KijiExpress job.
    |
    |Usage:
    |  %(this)s
    |    --libjars=path/to/jar1,path/to/jar2
    |    --user_jar=org.MyBigDataApp.jar
    |    --job_name=org.MyBigDataApp.MyJob
    |    --mode=local|hdfs
    |    --hadoop_ver=hadoop1|hadoop2
    |    --conf=<configuration file>
    |    --fs=<local|namenode:port>
    |    --jt=<local|jobtracker:port>
    |    --archives=<comma separated list of archives>
    |    --Dargs=property1=value,property1=value2,...
    |    arguments...
  """

  def RegisterFlags(self):
    self.flags.AddString(
        name='libjars',
        default="",
        help=('Comma separated list of third party jars to append/prepend to'
            'the java classpath. These take precedence over the jars from'
            'KIJI_CLASSPATH env var. Can be a directory path that holds jars.'),
    )
    self.flags.AddString(
        name='job_name',
        default="",
        help=('Java class name for the KijiExpress Job to be run.')
    )
    self.flags.AddString(
        name='user_jar',
        default="",
        help=('Jar that the user supplies to express command that contains the'
            'job they hope to run.'),
    )
    self.flags.AddString(
        name='mode',
        default='local',
        help=('Hadoop mode to run in. Must be hdfs or local..'),
    )
    self.flags.AddString(
        name='hadoop_ver',
        default="",
        help=('Either \'hadoop1\' or \'hadoop2\', use this to manually set the'
            'version of hadoop. If unset, script will resolve version by'
            'querying binary in $HADOOP_HOME directory.'),
    )
    self.flags.AddString(
        name='conf',
        default='',
        help=('Hadoop generic option, specifies an application configuration '
              'file.'),
    )
    self.flags.AddString(
        name='fs',
        default='',
        help=('Hadoop generic option, specifies a namenode.'),
    )
    self.flags.AddString(
        name='jt',
        default='',
        help=('Hadoop generic option, specifies a job tracker.'),
    )
    self.flags.AddString(
        name='archives',
        default='',
        help=('Hadoop generic option, comma separated archives to be '
              'unarchived on the compute machines.'),
    )
    self.flags.AddString(
        name='Dargs',
        default='',
        help=('Hadoop generic options. Command separated list of properties. '
            'They get passed to hadoop as -Dproperty1=val1 -Dproperty2=val2.'),
    )
    self.flags.AddString(
        name='hadoop_generic_options',
        default='',
        help=('A string passed to hadoop for hadoop\'s generic options. Added '
            'as is to java command launching job. Unspecified  behavior if '
            'express script flags also set hadoop generic options.'),
    )

  def java_options(self, env):

    """ Sets the java options by concatenating EXPRESS_JAVA_OPTS, JAVA_OPTS,
        JAVA_LIBRARY_PATH.

    Args:
      env: A dictionary mapping environment variables name to values.
    Returns:
      A list of args to get added to java command in order to modify the jvm.
    """
    java_opts = list()
    java_opts = self.add_string(
        java_opts, env.get('EXPRESS_JAVA_OPTS', ''), ' ')
    java_opts = self.add_string(java_opts, env.get('JAVA_OPTS', ''), ' ')

    # This is a workaround for OS X Lion, where a bug in JRE 1.6 creates a lot
    # of 'SCDynamicStore' errors.
    if env.get('uname') == 'Darwin':
      java_opts.append('-Djava.security.krb5.realm=')
      java_opts.append('-Djava.security.krb5.kdc=')

    def hadoop_native_libs(env):
      """Check for native libaries provided with hadoop distribution.

      Returns:
        A string that is the path to the native libaries.
      """
      java_library_path = ''
      if env.get('JAVA_LIBRARY_PATH') != None:
        java_library_path = env['JAVA_LIBRARY_PATH'].split(',')
      native_dir_path = os.path.join(env.get('HADOOP_HOME'), 'lib/native')
      if (os.path.isdir(native_dir_path)):
        hadoop_cp = self.classpath_from_executable(
            env, 'HADOOP_HOME', 'hadoop')
        # Hadoop wants a certain platform version, then we hope to use it
        cmd = ['java', '-cp', hadoop_cp, '-Xmx32m',
            'org.apache.hadoop.util.PlatformName']
        output = subprocess.check_output(cmd)
        java_platform = output.decode()
        native_dirs = os.path.join(native_dir_path,
            java_platform.replace(" ", "_"))
        if (os.path.isdir(native_dirs)):
          java_library_path = native_dirs
        else:
          java_library_path = ('%s:%s' %
              (env.get('JAVA_LIBRARY_PATH'), native_dir_path))
      return java_library_path

    lib_path = env.get('JAVA_LIBRARY_PATH', '')
    native_lib_path = hadoop_native_libs(env)
    if native_lib_path != '':
      lib_path = lib_path + ':' + native_lib_path
    if lib_path != '':
      java_opts.append('-Djava.library.path=%s' % (lib_path))

    # warn the user if there are conflicting options in environment variables
    opts = set()
    for opt in java_opts:
      key = opt.split('=')[0]
      if key in opts:
        logging.warning('The java option %s has been set multiple times. Check '
            'your EXPRESS_JAVA_OPTS and JAVA_OPTS environment variables.')
      else:
        opts.add(key)
    logging.debug('The java options passed to the jvm are: %s'
        % ('\n'.join(java_opts)))
    return java_opts

  def get_run_mode(self):
    """ Examine mode flag and return a command line argument for hadoop. Raises
      an exception in the event of trouble while parsing flag.

    Returns:
      Either 'hdfs' or 'local'.
    """
    mode = ''
    if self.flags.mode == 'hdfs':
      mode = '--hdfs'
    elif self.flags.mode == 'local':
      mode= '--local'
    else:
      raise Exception('Improperly set mode flag for express shell command.')
    logging.debug('Express will run a job in %s mode.' % (mode))
    return mode

  def hadoop_args(self, user_jar, hadoop_ver):
    """ Creates the arguments passed to the hadoop generic options parser.

    Args:
      user_jar: User specified jar containing class to run.
      hadoop_ver: Version of hadoop distribution, e.g. 'hadoop2'.
    Returns:
      A list to pass to the java -cp command.
    """
    args = []
    tmp_jar_str = self.tmpjar_classpath(self.env, hadoop_ver)
    args.append('-Dtmpjars=file://%s,%s' % (user_jar,tmp_jar_str))
    args.append('-Dmapreduce.task.classpath.user.precedence=true')
    if self.flags.conf != '':
      args.extend(['-conf', self.flags.conf])
    if self.flags.fs != '':
      args.extend(['-fs', self.flags.fs])
    if self.flags.jt != '':
      args.extend(['-jt', self.flags.jt])
    if self.flags.archives != '':
      self.add_to_list(args, self.flags.archives.split(','))
    if self.flags.Dargs != '':
      for arg in self.flags.Dargs.split(','):
        args.append(['-D%s' % (arg)])
    logging.debug('The hadoop generic options for the job are: %s' %
        ('\n'.join(args)))
    return args

  def java_cmd(
      self,
      classpath,
      java_opts,
      express_tool,
      hadoop_args,
      class_name,
      run_mode,
      user_args,
    ):
    """ Create a list to pass to python.subprocess to launch job.

    Args:
      classpath: Classpath of kiji project jars, hadoop, and hbase.
      java_opts: Java options to configure the jvm.
      express_tool: Express tool used to launch job.
      hadoop_args: Arguments for the hadoop generic parser.
      class_name: The class the user wants to run.
      run_mode: What hadoop mode to run, e.g. '--hdfs' or '--local'.
      user_args: A list of unparsed arguments to get sent to jar.
    """
    assert(type(classpath) == str)
    assert(type(class_name) == str)
    assert(type(hadoop_args) == list and len(hadoop_args) > 1)
    assert(type(run_mode) == str)
    assert(type(user_args) == list)
    cmd = ['java', '-cp']
    # make sure nothing is an empty string, necessary check for java_opts
    f = lambda x: cmd.append(x) if len(x) > 0 else cmd
    f(classpath)
    f(java_opts)
    # the following property only needed in kiji-schema v1.1
    f('-Dorg.kiji.schema.impl.AvroCellEncoder.SCHEMA_VALIDATION=DISABLED')
    f(express_tool)
    cmd.extend(hadoop_args)
    f(class_name)
    f(run_mode)
    if (len(user_args) == 1):
      cmd.append(user_args[0])
    elif (len(user_args) > 1):
      cmd.extend(user_args)
    return cmd

  def Run(self, args):
    """ Run Method. Launches an express job after building a classpath.

    Args:
      args: A python list representing the kiji-flags to pass to the Express Job
          launcher tool.
    Returns:
      Nothing.
    """
    self.check_env()
    if (self.flags.user_jar == ""):
      raise Exception('Please specify a  user_jar flag.')
    if (self.flags.job_name == ""):
      raise Exception('Please specify a  job_name flag.')
    # any more args must be user specified args for the express job
    user_args = list(args)
    logging.debug('After parsing flags the express script will pass the'
        'following args to the express job: %s' % (user_args))
    user_jar = os.path.abspath(self.flags.user_jar)
    hadoop_ver = self.hadoop_ver(self.env, self.flags.hadoop_ver)
    express_cp = self.create_classpath(self.env, hadoop_ver)
    log_list = list(map(lambda path: os.path.abspath(path), express_cp))
    logging.debug('The generated classpath is: %s' % ('\n'.join(log_list)))
    cp = user_jar + ':' + ":".join(express_cp)
    java_opts = ' '.join(self.java_options(self.env))
    hadoop_args = self.hadoop_args(user_jar, hadoop_ver)
    run_mode = self.get_run_mode()
    cmd = self.java_cmd(
        classpath=cp,
        java_opts=java_opts,
        express_tool=KIJI_EXPRESS_TOOL,
        hadoop_args=hadoop_args,
        class_name=self.flags.job_name,
        run_mode=run_mode,
        user_args=user_args,
    )
    logging.info('Trying to execute command:\n%s' % ('\n'.join(cmd)))
    subprocess.call(cmd)

class Shell(Command):
  USAGE = """
    |
    |Starts an interactive shell for running KijiExpress code.
    |
    |Usage:
    |  %(this)s
    |  --libjars=path/to/jar1,path/to/jar2
    |  --mode=local|hdfs
    |  --hadoop_ver=hadoop1|hadoop2
  """

  def RegisterFlags(self):
    self.flags.AddString(
        name='libjars',
        default="",
        help=('Comma separated list of third party jars to append/prepend to'
            'the java classpath. These take precedence over the jars from'
            'KIJI_CLASSPATH env var. Can be a directory path that holds jars.'),
    )
    self.flags.AddString(
        name='mode',
        default='local',
        help=('Hadoop mode to run in. Must be hdfs or local.'),
    )
    self.flags.AddString(
        name='hadoop_ver',
        default="",
        help=('Either \'hadoop1\' or \'hadoop2\', use this to manually set the'
            'version of hadoop. If unset, script will resolve version by'
            'querying binary in $HADOOP_HOME directory.'),
    )

  def run_mode_specific_script(self):
    """ The express shell needs a scala script specific to the mode it runs in.

    Returns:
      A path to the run mode specific scala script.
    """
    script_path = ''
    if self.flags.mode == 'hdfs':
      hdfs_path = os.path.join(os.path.realpath(__file__), 'hdfs-mode.scala')
      script_path = hdfs_path
    elif self.flags.mode == 'local':
      local_path = os.path.join(os.path.realpath(__file__), 'local-mode.scala')
      script_path = local_path
    else:
      raise Exception('Improperly set mode flag for express shell command.')
    logging.info('Using the script %s as the mode specific script for express '
        'shell.' % (script_path))
    return script_path

  def Run(self, args):
    self.check_env()
    # export classpath for schema-shell to use
    hadoop_ver = self.hadoop_ver(self.env, self.flags.hadoop_ver)
    express_cp = self.create_classpath(self.env, hadoop_ver)
    tmp_jar_str = self.tmpjar_classpath(self.env, hadoop_ver)
    # EXPRESS_MODE env var must be a path to the mode specific scala script
    env = self.env
    self.env.update({
      'EXPRESS_CP' : ":".join(express_cp),
      'TMPJARS' : tmp_jar_str,
      'EXPRESS_MODE' : self.run_mode_specific_script()
    })
    # express shell binary needs to be in the same directory as this script
    shell_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), 'express-shell')
    cmd = [shell_path]
    cmd.extend(list(args))
    logging.info('Launching the express shell using the command: %s' % (cmd))
    proc = subprocess.Popen(cmd, env=self.env)
    try:
      proc.wait()
    except:
      proc.kill()


class Schema_Shell(Command):

  USAGE = """
    |
    |Starts KijiSchema Shell loaded with KijiExpress extensions.
    |
    |Usage:
    |  %(this)s --hadoop_ver=hadoop1|hadoop2
  """

  def RegisterFlags(self):
    self.flags.AddString(
        name='libjars',
        default="",
        help=('Comma separated list of third party jars to append/prepend to'
            'the java classpath. These take precedence over the jars from'
            'KIJI_CLASSPATH env var. Can be a directory path that holds jars.'),
    )
    self.flags.AddString(
        name='hadoop_ver',
        default="",
        help=('Either \'hadoop1\' or \'hadoop2\', use this to manually set the'
            'version of hadoop. If unset, script will resolve version by'
            'querying binary in $HADOOP_HOME directory.'),
    )

  def schema_java_options(self, hadoop_ver):
    """ Set jvm options for launching the schema-shell.

    Returns:
      A list to be used with subprocess module.
    """
    args = []
    java_opts = self.env.get('JAVA_OPTS')
    if java_opts != None:
      self.add_to_list(args, java_opts.split(','))
    tmp_jar_str = self.tmpjar_classpath(self.env, hadoop_ver)
    args.append('-Dexpress.tmpjars=%s' % (tmp_jar_str))
    args.append(
        '-Dorg.kiji.schema.impl.AvroCellEncoder.SCHEMA_VALIDATION=DISABLED')
    return args

  def Run(self, args=''):
    self.check_env()
    cp = []
    schema_shell_script = os.path.join(
        self.env.get('SCHEMA_SHELL_HOME'), 'bin/kiji-schema-shell')
    kiji_cp_str = self.env.get('KIJI_CLASSPATH')
    if kiji_cp_str != None:
      self.add_to_list(cp, kiji_cp.split(','))
    hadoop_ver = self.hadoop_ver(self.env, self.flags.hadoop_ver)
    express_cp = self.create_classpath(self.env, hadoop_ver)
    cp.extend(express_cp)
    java_opts = self.schema_java_options(hadoop_ver)
    self.env.update({
        'KIJI_CLASSPATH' : ':'.join(cp),
        'JAVA_OPTS' : ' '.join(java_opts),
    })
    cmd = [schema_shell_script]
    logging.info('Using %s to launch schema-shell.' % (cmd))
    proc = subprocess.Popen(cmd, env=self.env)
    try:
      proc.wait()
    except:
      proc.kill()


# ------------------------------------------------------------------------------


# Usage string for script, used by the help command.
USAGE = """\
Usage:
  %(this)s [--do]=command [--flags ...] arguments...

Commands:
  help            - Displays this help message.
  shell           - Launches interactive KijiExpress shell.
  schema-shell    - Starts KijiSchema Shell loaded with KijiExpress extensions.
  job             - Java exec a compiled KijiExpress job.
  jar             - Runs arbitrary Scala or Java program with express classpath.
  classpath       - Prints the classpath used to run KijiExpress.

Environment Variables:
  EXPRESS_JAVA_OPTS  - Extra arguments to pass to the KijiExpress's JVM.
  KIJI_CLASSPATH     - Colon-separated jars for classpath and distributed cache.
  JAVA_LIBRARY_PATH  - Colon-separated paths to additional native libs.
  JAVA_OPTS          - Java args to append to java command and sent to JVM.
"""


class Help(Command):
  USAGE = """
    |
    |Displays this help message.
    |
    |Usage:
    |  %(this)s
  """

  def Run(self, args):
    this = base.GetProgramName()
    print(this)
    if (len(args) == 1) and (args[0] in COMMANDS):
      command = args[0]
      print('Usage for %s' % (command))
      COMMANDS[command](['--help'])
    else:
      print(USAGE % {'this': this})


# ------------------------------------------------------------------------------
COMMANDS = dict(
    map(lambda cls: (base.UnCamelCase(cls.__name__, separator='-'), cls),
        (Command).__subclasses__()))

FLAGS.AddString(
    name='do',
    default=None,
    help=('Action to perform:\n%s.'
          % ',\n'.join(map(lambda name: ' - ' + name, sorted(COMMANDS)))),
)

def Main(args):
  """Program entry point.

  Args:
    args: unparsed command-line arguments.
  """
  action_name = FLAGS.do
  if (action_name is None) and (len(args) >= 1) and (args[0] in COMMANDS):
    action_name = args[0]
    args = args[1:]
  elif (len(args) == 0):
    action_name = 'help'
  action_class = COMMANDS.get(action_name)
  assert(action_class is not None), ('Command name received: {}. Could not '
      'find command name. Use help to find valid commands.'.format(args[0]))
  logging.debug('Actions class found is: %s' % (action_class))
  if (action_class != None):
    action = action_class()
    return action(list(args))


# ------------------------------------------------------------------------------


if __name__ == '__main__':
  base.Run(Main)