#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-
"""Runs the KijiExpress Music tutorial."""

import glob
import logging
import os
import shutil
import sys
import tempfile

# Add the root directory to the Python path if necessary:
__path = os.path.dirname(os.path.dirname(os.path.abspath(sys.argv[0])))
if __path not in sys.path:
  sys.path.append(__path)

from base import base
from kiji import kiji_bento
from kiji import tutorial_test


FLAGS = base.FLAGS
LogLevel = base.LogLevel


class Error(Exception):
  """Errors used in this module."""
  pass


# Horizontal ruler:
LINE = '-' * 80


# ------------------------------------------------------------------------------


FLAGS.AddString(
    name='work_dir',
    help='Working directory.',
)

FLAGS.AddString(
    name='maven_local_repo',
    help='Optional Maven local repository from where to fetch artifacts.',
)

FLAGS.AddString(
    name='maven_remote_repo',
    help='Optional Maven remote repository from where to fetch artifacts.',
)

FLAGS.AddString(
    name='kiji_bento_version',
    help=('Version of KijiBento to download and test against. '
          'For example "1.0.0-rc4" or "1.0.0-rc5-SNAPSHOT".'),
)

FLAGS.AddBoolean(
    name='cleanup_after_test',
    default=True,
    help=('When set, disables cleaning up after test. '
          'Bento cluster stay alive, working directory is not wiped.'),
)

FLAGS.AddBoolean(
    name='help',
    default=False,
    help='Prints a help message.',
)


# ------------------------------------------------------------------------------


class Tutorial(object):
  """Runs the KijiMusic tutorial."""

  def __init__(
      self, work_dir, version,
      maven_local_repo=None,
      maven_remote_repo=None):
    """Initializes the tutorial runner.

    Args:
      work_dir: Working directory where to operate.
      version: Version of KijiBento to test, eg. '1.0.0-rc5-SNAPSHOT'.
      maven_local_repo: Optional local Maven repository.
      maven_remote_repo: Optional remote Maven repository.
    """
    self._work_dir = work_dir
    self._run_id = base.NowMS()
    self._kiji_version = version

    # TODO: inject these in KijiBento
    self._maven_local_repo = maven_local_repo
    self._maven_remote_repo = maven_remote_repo

    # Initialized in Setup()
    self._kiji_bento = kiji_bento.KijiBento(
        path=os.path.join(self.work_dir, 'kiji-bento-%s' % self._kiji_version),
        version=self._kiji_version,
    )

  @property
  def work_dir(self):
    """Returns the working directory."""
    return self._work_dir

  @property
  def kiji_bento(self):
    """Returns the KijiBento install."""
    return self._kiji_bento

  @property
  def bento_cluster(self):
    """Returns the BentoCluster install."""
    return self.kiji_bento.bento_cluster

  def Setup(self):
    """Initializes the tutorial runner.

    Fetches the KijiBento Maven artifact, unzip it, starts a Bento cluster,
    and prepares a working environment.
    """
    self.kiji_bento.Install()
    self.bento_cluster.Start()

    self._express_music_dir = (
        os.path.join(self.kiji_bento.path, 'examples', 'express-music'))
    assert os.path.exists(self._express_music_dir), (
        'KijiExpress tutorial root directory not found: %r' % self._express_music_dir)

    self._hdfs_base = 'express-music-%d' % self._run_id
    self._kiji_instance_uri = 'kiji://.env/kiji_music_%d' % self._run_id

    express_music_lib_dir = os.path.join(self._express_music_dir, 'lib')

    # Find the jar for kiji-express-music in the lib dir.
    music_jars = glob.glob("{}/{}".format(express_music_lib_dir, "kiji-express-music-*.jar"))
    assert (len(music_jars) == 1)
    express_music_jar = music_jars[0]

    # Builds a working environment for KijiMusic tutorial commands:
    self._env = dict(os.environ)
    self._env.update({
        'MUSIC_EXPRESS_HOME': self._express_music_dir,
        'LIBS_DIR': express_music_lib_dir,
        'EXPRESS_MUSIC_JAR': express_music_jar,
        'KIJI': self._kiji_instance_uri,
        'KIJI_CLASSPATH':
            ':'.join(glob.glob(os.path.join(express_music_lib_dir, '*'))),
        'HDFS_BASE': self._hdfs_base,
    })

  def Command(self, command):
    """Runs a Kiji command-line.

    Args:
      command: Kiji command-line to run as a single string.
    """
    cmd = tutorial_test.KijiCommand(
        command=command,
        work_dir=self.kiji_bento.path,
        env=self._env,
    )
    logging.debug('Exit code: %d', cmd.exit_code)
    if logging.getLogger().level <= LogLevel.DEBUG_VERBOSE:
      logging.debug('Output:\n%s\n%s%s', LINE, cmd.output_text, LINE)
      logging.debug('Error:\n%s\n%s%s', LINE, cmd.error_text, LINE)
    else:
      logging.debug('Output: %r', cmd.output_text)
      logging.debug('Error: %r', cmd.error_text)
    return cmd

  # ----------------------------------------------------------------------------
  # KijiMusic setup:

  def Part1(self):
    """Runs the setup part of the KijiExpress Music tutorial.

    http://docs.kiji.org/tutorials/express-recommendation/DEVEL/express-setup/
    """

    # --------------------------------------------------------------------------

    install = self.Command('kiji install --kiji=${KIJI}')
    assert (install.exit_code == 0)
    assert ('Successfully created kiji instance: ' in install.output_text)

    # --------------------------------------------------------------------------

    create_table = self.Command(base.StripMargin("""
        |kiji-schema-shell \\
        |    --kiji=${KIJI} \\
        |    --file=${MUSIC_EXPRESS_HOME}/music-schema.ddl \\
        """))
    print(create_table.error_text)
    assert (create_table.exit_code == 0)

    # --------------------------------------------------------------------------

    list_tables = self.Command('kiji ls ${KIJI}')
    assert (list_tables.exit_code == 0)
    assert ('songs' in list_tables.output_text), (
        'Missing table "songs": %s' % list_tables.output_lines)
    assert ('users' in list_tables.output_text), (
        'Missing table "users": %s' % list_tables.output_lines)

    # --------------------------------------------------------------------------

    mkdir = self.Command('hadoop fs -mkdir ${HDFS_BASE}/express-tutorial/')
    assert (mkdir.exit_code == 0)

    copy = self.Command(base.StripMargin("""
        |hadoop fs -copyFromLocal \\
        |    ${MUSIC_EXPRESS_HOME}/example_data/*.json \\
        |    ${HDFS_BASE}/express-tutorial/
        """))
    assert (copy.exit_code == 0)

  # ----------------------------------------------------------------------------
  # KijiExpress Music bulk-importing:

  def Part2(self, python=False):
    """Runs the importing part of the KijiExpress Music tutorial.

    http://docs.kiji.org/tutorials/express-recommendation/DEVEL/express-importing-data/
    """

    # --------------------------------------------------------------------------

    cmd = ' '
    if python == False:
      cmd = base.StripMargin("""
          |express job --libjars "${MUSIC_EXPRESS_HOME}/lib/*" \\
          |    ${EXPRESS_MUSIC_JAR} \\
          |    org.kiji.express.music.SongMetadataImporter \\
          |    --input ${HDFS_BASE}/express-tutorial/song-metadata.json \\
          |    --table-uri ${KIJI}/songs --hdfs
          """)

    else:
      cmd = base.StripMargin("""
          |python3 /home/sea_bass/developer/workspace/kiji-end-to-end-tests/express_script.py \\
          |    job \\
          |    --libjars="${MUSIC_EXPRESS_HOME}/lib/*" \\
          |    --user_jar=${EXPRESS_MUSIC_JAR} \\
          |    --job_name=org.kiji.express.music.SongMetadataImporter \\
          |    --mode=hdfs \\
          |    --input ${HDFS_BASE}/express-tutorial/song-metadata.json \\
          |    --table-uri ${KIJI}/songs
          """)

    songMetadataImport = self.Command(cmd)
    assert (songMetadataImport.exit_code == 0)

    # --------------------------------------------------------------------------

    list_rows = self.Command('kiji scan ${KIJI}/songs --max-rows=5')
    assert (list_rows.exit_code == 0)
    assert (list_rows.output_lines[0].startswith('Scanning kiji table: kiji://'))
    assert (len(list_rows.output_lines) >= 3 * 5 + 1), len(list_rows.output_lines)
    for row in range(0, 5):
      tutorial_test.ExpectRegexMatch(
          expect=r"^entity-id=\['song-\d+'\] \[\d+\] info:metadata$",
          actual=list_rows.output_lines[1 + row * 3])
      tutorial_test.ExpectRegexMatch(
          expect=r"^\s*{\s*\"song_name\".*\"album_name\".*\"artist_name\".*\"genre\".*\"tempo\".*\"duration\".*\s*}\s*$",
          actual=list_rows.output_lines[2 + row * 3])
      tutorial_test.ExpectRegexMatch(
          expect=r"^$",
          actual=list_rows.output_lines[3 + row * 3])

    # --------------------------------------------------------------------------

    cmd = ' '
    if python == False:
      cmd = base.StripMargin("""
          |express job --libjars "${MUSIC_EXPRESS_HOME}/lib/*" \\
          |    ${EXPRESS_MUSIC_JAR} \\
          |    org.kiji.express.music.SongPlaysImporter \\
          |    --input ${HDFS_BASE}/express-tutorial/song-plays.json \\
          |    --table-uri ${KIJI}/users --hdfs
          """)
    else:
      cmd = base.StripMargin("""
        |python3 /home/sea_bass/developer/workspace/kiji-end-to-end-tests/express_script.py \\
        |    job \\
        |    -libjars="${MUSIC_EXPRESS_HOME}/lib/*" \\
        |    -user_jar=${EXPRESS_MUSIC_JAR} \\
        |    -job_name=org.kiji.express.music.SongPlaysImporter \\
        |    -mode=hdfs \\
        |    --input ${HDFS_BASE}/express-tutorial/song-plays.json \\
        |    --table-uri ${KIJI}/users
        """)
    userDataImport = self.Command(cmd)
    assert (userDataImport.exit_code == 0)

    # --------------------------------------------------------------------------

    list_rows = self.Command('kiji scan ${KIJI}/users --max-rows=5')
    assert (list_rows.exit_code == 0)
    assert (list_rows.output_lines[0].startswith('Scanning kiji table: kiji://'))
    assert (len(list_rows.output_lines) >= 3 * 5 + 1), len(list_rows.output_lines)
    for row in range(0, 5):
      tutorial_test.ExpectRegexMatch(
          expect=r"^entity-id=\['user-\d+'\] \[\d+\] info:track_plays$",
          actual=list_rows.output_lines[1 + row * 3])
      tutorial_test.ExpectRegexMatch(
          expect=r"^\s*song-\d+$",
          actual=list_rows.output_lines[2 + row * 3])
      tutorial_test.ExpectRegexMatch(
          expect=r"^$",
          actual=list_rows.output_lines[3 + row * 3])

  # --------------------------------------------------------------------------
  # play-count section.

  def Part3(self, python=False):
    cmd = ' '
    if python == False:
      cmd = base.StripMargin("""
        |express job --libjars "${MUSIC_EXPRESS_HOME}/lib/*" \\
        |  ${EXPRESS_MUSIC_JAR} \\
        |  org.kiji.express.music.SongPlayCounter \\
        |  --table-uri ${KIJI}/users \\
        |  --output ${HDFS_BASE}/express-tutorial/songcount-output \\
        |  --hdfs
        """)
    else:
      cmd = base.StripMargin("""
        |python3 /home/sea_bass/developer/workspace/kiji-end-to-end-tests/express_script.py \\
        |    job \\
        |    -libjars="${MUSIC_EXPRESS_HOME}/lib/*" \\
        |    -user_jar=${EXPRESS_MUSIC_JAR} \\
        |    -job_name=org.kiji.express.music.SongPlayCounter \\
        |    -mode=hdfs \\
        |    --table-uri ${KIJI}/users \\
        |    --output ${HDFS_BASE}/express-tutorial/songcount-output \\
        """)
    play_count = self.Command(cmd)
    assert (play_count.exit_code == 0)
    fs_text = self.Command("""
        hadoop fs -text ${HDFS_BASE}/express-tutorial/songcount-output/part-00000 | head -3
        """)
    tutorial_test.Expect(expect=0, actual=fs_text.exit_code)
    lines = list(filter(None, fs_text.output_lines))  # filter empty lines
    tutorial_test.Expect(expect=3, actual=len(lines))
    for line in lines:
      tutorial_test.ExpectRegexMatch(expect=r'^song-\d+\t\d+$', actual=line)

  # ----------------------------------------------------------------------------
  # Top Next Songs section.
  def Part4(self, python=False):
    cmd = ' '
    if python is False:
      cmd = base.StripMargin("""
        |express job --libjars "${MUSIC_EXPRESS_HOME}/lib/*" \\
        |    ${EXPRESS_MUSIC_JAR} \\
        |    org.kiji.express.music.TopNextSongs \\
        |    --users-table ${KIJI}/users \\
        |    --songs-table ${KIJI}/songs --hdfs
        """)
    else:
      cmd = base.StripMargin("""
        |python3 /home/sea_bass/developer/workspace/kiji-end-to-end-tests/express_script.py \\
        |    job \\
        |    -libjars="${MUSIC_EXPRESS_HOME}/lib/*" \\
        |    -user_jar=${EXPRESS_MUSIC_JAR} \\
        |    -job_name=org.kiji.express.music.TopNextSongs \\
        |    -mode=hdfs \\
        |    --users-table ${KIJI}/users \\
        |    --songs-table ${KIJI}/songs --hdfs
        """)
    top_songs = self.Command(cmd)
    assert (top_songs.exit_code == 0)
    list_rows = self.Command('kiji scan ${KIJI}/songs --max-rows=2')
    assert (list_rows.exit_code == 0)
    assert (list_rows.output_lines[0].startswith('Scanning kiji table: kiji://'))
    assert (len(list_rows.output_lines) >= 5 * 2 + 1), len(list_rows.output_lines)
    for row in range(0, 2):
      tutorial_test.ExpectRegexMatch(
          expect=r"^entity-id=\['song-\d+'\] \[\d+\] info:metadata$",
          actual=list_rows.output_lines[1 + row * 5])
      tutorial_test.ExpectRegexMatch(
          expect=r"^\s*{\s*\"song_name\".*\"album_name\".*\"artist_name\".*\"genre\".*\"tempo\".*\"duration\".*\s*}\s*$",
          actual=list_rows.output_lines[2 + row * 5])
      tutorial_test.ExpectRegexMatch(
          expect=r"^entity-id=\['song-\d+'\] \[\d+\] info:top_next_songs$",
          actual=list_rows.output_lines[3 + row * 5])
      tutorial_test.ExpectRegexMatch(
          expect=r"^\s*{\s*\"top_songs\".*}$",
          actual=list_rows.output_lines[4 + row * 5])
      tutorial_test.ExpectRegexMatch(
          expect=r"^$",
          actual=list_rows.output_lines[5 + row * 5])

  # ----------------------------------------------------------------------------
  # Song recommender
  def Part5(self, python=False):
    cmd = ' '
    if python == False:
      cmd = base.StripMargin("""
        |express job ${EXPRESS_MUSIC_JAR} \\
        |    org.kiji.express.music.SongRecommender \\
        |    --songs-table ${KIJI}/songs \\
        |    --users-table ${KIJI}/users
        """)
    else:
      cmd = base.StripMargin("""
        |python3 /home/sea_bass/developer/workspace/kiji-end-to-end-tests/express_script.py \\
        |    job \\
        |    -user_jar=${EXPRESS_MUSIC_JAR} \\-b 
        |    -job_name=org.kiji.express.music.SongRecommender \\
        |    -mode=hdfs \\
        |    --songs-table ${KIJI}/songs \\
        |    --users-table ${KIJI}/users
        """)
    song_recommend = self.Command(cmd)
    assert (song_recommend.exit_code == 0)

    list_rows = self.Command("kiji scan ${KIJI}/users --max-rows=2")
    assert (list_rows.exit_code == 0)
    assert (list_rows.output_lines[0].startswith('Scanning kiji table: kiji://'))
    assert (len(list_rows.output_lines) >= 5 * 2 + 1), len(list_rows.output_lines)
    for row in range(0, 2):
      tutorial_test.ExpectRegexMatch(
          expect=r"^entity-id=\['user-\d+'\] \[\d+\] info:track_plays$",
          actual=list_rows.output_lines[1 + row * 5])
      tutorial_test.ExpectRegexMatch(
          expect=r"^\s*song-\d+$",
          actual=list_rows.output_lines[2 + row * 5])
      tutorial_test.ExpectRegexMatch(
          expect=r"^entity-id=\['user-\d+'\] \[\d+\] info:next_song_rec$",
          actual=list_rows.output_lines[3 + row * 5])
      tutorial_test.ExpectRegexMatch(
          expect=r"^\s*song-\d+$",
          actual=list_rows.output_lines[4 + row * 5])
      tutorial_test.ExpectRegexMatch(
          expect=r"^$",
          actual=list_rows.output_lines[5 + row * 5])


  # ----------------------------------------------------------------------------
  # Cleanup:

  def Cleanup(self):
    self.bento_cluster.Stop()
    shutil.rmtree(self.work_dir)


# ------------------------------------------------------------------------------

def Main(args):
  """Program entry point."""
  if FLAGS.help:
    FLAGS.PrintUsage()
    return os.EX_OK

  if len(args) > 0:
    logging.error('Unexpected command-line arguments: %r' % args)
    FLAGS.PrintUsage()
    return os.EX_USAGE

  # Create a temporary working directory:
  cwd = os.getcwd()
  work_dir = FLAGS.work_dir
  if work_dir is None:
    work_dir = tempfile.mkdtemp(prefix='work_dir.', dir=os.getcwd())
  work_dir = os.path.abspath(work_dir)
  if not os.path.exists(work_dir):
    os.makedirs(work_dir)
  FLAGS.work_dir = work_dir

  logging.info('Working directory: %r', work_dir)

  if not FLAGS.kiji_bento_version:
    print('Specify the version of KijiBento to test '
        + 'with --kiji_bento_version=...')
    return os.EX_USAGE
  logging.info('Testing tutorial of KijiBento %s', FLAGS.kiji_bento_version)

  # Runs the tutorial:
  tutorial = Tutorial(
      work_dir=work_dir,
      version=FLAGS.kiji_bento_version,
      maven_local_repo=FLAGS.maven_local_repo,
      maven_remote_repo=FLAGS.maven_remote_repo,
  )

  try:
    tutorial.Setup()
    tutorial.Part1()
    tutorial.Part2()
    tutorial.Part3()
    tutorial.Part4()
    tutorial.Part5()
  finally:
    if FLAGS.cleanup_after_test:
      tutorial.Cleanup()

  try:
    tutorial.Setup()
    tutorial.Part1()
    tutorial.Part2(python=True)
    tutorial.Part3(python=True)
    tutorial.Part4(python=True)
    tutorial.Part5(python=True)
  finally:
    if FLAGS.cleanup_after_test:
      tutorial.Cleanup()


# ------------------------------------------------------------------------------


if __name__ == '__main__':
  base.Run(Main)
