#!/usr/bin/env bash
#
#   (c) Copyright 2013 WibiData, Inc.
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
# This script is run in an empty environment by the upgrade tool from the
# previous BentoBox. It must configure the new cluster and move it in place
# on top of the previous BentoBox, after moving that one to an archive
# directory.
#
# Arguments:
# $1 = PREV_BENTO_ROOT - path to the previous bento box.
# $2 = PREV_VERSION - previous BentoBox version.

function resolve_symlink() {
  TARGET_FILE=$1

  if [ -z "$TARGET_FILE" ]; then
    echo ""
    return 0
  fi

  cd $(dirname "$TARGET_FILE")
  TARGET_FILE=$(basename "$TARGET_FILE")

  # Iterate down a (possible) chain of symlinks
  count=0
  while [ -L "$TARGET_FILE" ]; do
      if [ "$count" -gt 1000 ]; then
        # Just stop here, we've hit 1,000 recursive symlinks. (cycle?)
        break
      fi

      TARGET_FILE=$(readlink "$TARGET_FILE")
      cd $(dirname "$TARGET_FILE")
      TARGET_FILE=$(basename "$TARGET_FILE")
      count=$(( $count + 1 ))
  done

  # Compute the canonicalized name by finding the physical path
  # for the directory we're in and appending the target file.
  PHYS_DIR=$(pwd -P)
  RESULT="$PHYS_DIR/$TARGET_FILE"
  echo "$RESULT"
}

prgm="$0"
prgm=`resolve_symlink "$prgm"`
bin=`dirname "$prgm"`

PREV_BENTO_ROOT="$1"
PREV_VERSION="$2"

if [ -z "$PREV_BENTO_ROOT" ]; then
  echo "Missing argument 1: Path to previous BentoBox"
  exit 1
fi

if [ -z "$PREV_VERSION" ]; then
  echo "Missing argument 2: Previous BentoBox version number"
  exit 1
fi

# Use strict mode; bail out if there are any errors along the way.
# We use error recovery handlers further down to unwind our actions if possible.
set -e

# We are in $BENTO_HOME/cluster/bin/.
NEW_BENTO_ROOT="${bin}/../../"
NEW_BENTO_ROOT=`cd "$NEW_BENTO_ROOT" && pwd`
NEW_BENTO_ROOT=`resolve_symlink "$NEW_BENTO_ROOT"`

# The 'mv' commands below will not run correctly if relative path names are included
# in the bento rootdir variables. We need to resolve these to absolute canonical
# names.

PREV_BENTO_ROOT=`cd "$PREV_BENTO_ROOT" && pwd`
PREV_BENTO_ROOT=`resolve_symlink "$PREV_BENTO_ROOT"`

if [ "$PREV_BENTO_ROOT" == "$NEW_BENTO_ROOT" ]; then
  echo "Cannot upgrade a BentoBox by archiving itself."
  echo "Are you trying to run upgrade-kiji-bootstrap.sh yourself?"
  echo "Run 'bento upgrade' to upgrade this instance instead."
  exit 1
fi

# BENTO-35: To avoid kiji file proliferation in the user's home directory, let's create
# a $HOME/.kiji folder and move/symlink files as necessary there.
HIDDEN_KIJI_FOLDER=${HOME}/.kiji

if [ ! -e ${HIDDEN_KIJI_FOLDER} ]; then
  mkdir -p ${HIDDEN_KIJI_FOLDER}
fi

# TODO: BENTO-41: Move timestamp and uuid files to $HIDDEN_KIJI_FOLDER and link back to
# keep hidden kiji files in a consistent location.

# If the disable-checkin file exists in the old bento's conf/ folder, then touch the
# new location's disable-checkin file until we can deprecate the old location.
if [ -e $PREV_BENTO_ROOT/conf/disable-checkin ]; then
  touch ${HIDDEN_KIJI_FOLDER}/.disable_kiji_checkin
  touch $NEW_BENTO_ROOT/conf/disable-checkin
fi

cd "$NEW_BENTO_ROOT"

# Load the new Bento environment within this script.
# Don't print env var information to the console.
source "./bin/kiji-env.sh" 2>&1 >/dev/null

echo ""
echo "Now setting up your new BentoBox. This may take a minute."
echo "Please do not interrupt this process."
echo "  * Your existing BentoBox will be moved to the archive/ subdir."
echo "  * A copy of your data and configuration will be archived too."
echo "  * The new BentoBox will be put in place of the old one."
echo "  * Your data and configuration will be imported."
echo ""
echo "Please wait..."
echo ""

# A function that is useful to call when debugging this script.
# Works after the sourcing of kiji-env.sh (i.e., here and below).
function dump_state_vars() {
  echo "program: $prgm"
  echo "bin: $bin"
  echo "Current working dir:"
  pwd
  echo ""
  echo "USER: $USER"
  echo "HOME: $HOME"
  echo "KIJI_HOME: $KIJI_HOME"
  echo "BENTO_CLUSTER_HOME: $BENTO_CLUSTER_HOME"
  echo ""
  echo "NEW_BENTO_ROOT: $NEW_BENTO_ROOT"
  echo "PREV_BENTO_ROOT: $PREV_BENTO_ROOT"
  echo "PREV_VERSION: $PREV_VERSION"
}

# If we encounter an error moving the old BentoBox's archive/ dir into the new
# archive dir, move it back...
function unwind_old_archives() {
  echo "I moved your BentoBox's archive/ dir out of the way to perform the upgrade."
  echo "Something went wrong; moving it back..."

  set +e
  mv "$NEW_BENTO_ROOT"/archive/* "$PREV_BENTO_ROOT/archive"
  if [ "$?" != 0 ]; then
    echo "Something went wrong during recovery. Your old version archives may be"
    echo "in the directory: $NEW_BENTO_ROOT/archive. You should 'mv' them back."
    exit 1
  fi

  echo "Error recovery completed."
  exit 1
}

# If we encounter an error after moving the old BentoBox to the temporary directory,
# move it all back so the user never noticed it was missing.
function unwind_move_to_archive() {
  echo "I moved your BentoBox out of the way to perform the upgrade."

  # Check that we didn't have an error calculating where the files went.
  if [ -z "$archive_dest" ]; then
    echo "Something went wrong and I don't know where it went."
    echo "You should 'ls' in your BentoBox to see if files are missing."
    echo "If so, check for them in a subdir of $NEW_BENTO_ROOT/archive."
    exit 1
  fi

  echo "Something went wrong; moving it back..."
  set +e

  mv "$archive_dest"/* "$PREV_BENTO_ROOT/"
  if [ "$?" != 0 ]; then
    echo "Something went wrong during recovery. Your old BentoBox may be"
    echo "in the directory: $archive_dest. You should 'mv' it back."
    exit 1
  fi
  mkdir "$PREV_BENTO_ROOT/archive"
  mv "$NEW_BENTO_ROOT"/archive/* "$PREV_BENTO_ROOT/archive"
  if [ "$?" != 0 ]; then
    echo "Something went wrong during recovery. Your prior BentoBoxes may be in"
    echo "the directory: $NEW_BENTO_ROOT/archive. You should 'mv' them back to"
    echo "$PREV_BENTO_ROOT/archive."
    exit 1
  fi

  echo "Error recovery completed."
  exit 1
}

# A function that can override the unwind_archive_move() as the error
# handler after everything's safe again.
function do_nothing() {
  echo ""
}


# Check that this bootstrap script hasn't been run already.
if [ -f "$BENTO_CLUSTER_HOME/.bootstrap_runonce" ]; then
  echo "This bootstrap script has already been run!"
  echo "To prevent damage to this cluster instance, this script will not be run."
  echo "If this is in error, try removing $BENTO_CLUSTER_HOME/.bootstrap_runonce"
  exit 1
fi
touch "$BENTO_CLUSTER_HOME/.bootstrap_runonce"

# Make an archive/ directory to hold old BentoBox instances.
mkdir "$NEW_BENTO_ROOT/archive"

# Paranoia: Make sure we can write to this directory.
touch "$NEW_BENTO_ROOT/archive/upgrade-write-test"
if [ ! -f "$NEW_BENTO_ROOT/archive/upgrade-write-test" ]; then
  echo "It doesn't seem like I can write inside the new unpacked instance. Cannot upgrade."
  exit 1
fi

rm "$NEW_BENTO_ROOT/archive/upgrade-write-test"

# Paranoia: Test for permission to write and delete in the previous BentoBox we're upgrading.
touch "$PREV_BENTO_ROOT/upgrade-write-test"

if [ ! -f "$PREV_BENTO_ROOT/upgrade-write-test" ]; then
  echo "It doesn't seem like I can modify your existing BentoBox. Do you have permission?"
  echo "Cannot run upgrade."
  exit 1
fi

rm "$PREV_BENTO_ROOT/upgrade-write-test"

# If we have an error moving the previous bento box's archive directory into the
# temp dir, move it back.
trap unwind_old_archives ERR

# Note that after this point, we don't 'exit 1'. We call the 'false' program,
# which will return with exit status 1, as well as trigger our error trap functions.

# Also, specify a trap handler for ^C; this does not need to do anything, but will
# trigger the error function to do its thing as well.
trap do_nothing SIGINT SIGTERM

# The previous BentoBox may have had an archive dir as well. Move that here...
if [ -d "$PREV_BENTO_ROOT/archive" ]; then
  # Previous archive dir exists...
  prev_versions=`ls -A "$PREV_BENTO_ROOT/archive"`
  if [ ! -z "$prev_versions" ]; then
    # And it's non-empty.
    mv "$PREV_BENTO_ROOT"/archive/* "$NEW_BENTO_ROOT/archive"
  fi
  rmdir "$PREV_BENTO_ROOT/archive"
fi

# Move the previous BentoBox into our archive dir
archive_dest="$NEW_BENTO_ROOT/archive/kiji-bento-$PREV_VERSION"
if [ -d "$archive_dest" ]; then
  # When copying the previous archive subdirs into our archive dir,
  # we wound up with an existing directory with the same name we want
  # to use for the most recent BentoBox.
  archive_suffix=$(date +%s)
  archive_dest="${archive_dest}.${archive_suffix}"
  mkdir -p ${archive_dest}
else
  # It doesn't exist; create the "normal" archive destination dir.
  mkdir "$archive_dest"
fi

archive_dest_base=`basename "$archive_dest"`

# If we encounter an error here, move the whole BentoBox back.
trap unwind_move_to_archive ERR

# Move the files from the previous BentoBox into a subdir of our archive/ directory.
# Don't move the "entire" previous BentoBox in, because we want to preserve the
# ownership, permissions, etc. of that directory as-is. We may not have write access
# to the parent dir of $PREV_BENTO_ROOT, so we leave $PREV_BENTO_ROOT existing but
# empty at the end of this process.
mv "$PREV_BENTO_ROOT"/* "$archive_dest"

# Import the previous BentoBox's state.
if [ -d "$archive_dest/cluster/state" ]; then
  bento import --src="$archive_dest"
fi

# Copy user config files from the old BentoBox into this one.
# Ignore the bento-*-site.xml files; they'll be autoregenerated by the new
# BentoBox the first time you start the servers.

old_hadoop_dir=`find "$archive_dest/cluster/lib/" -maxdepth 1 -name "hadoop*" -type d`
if [ -z "$old_hadoop_dir" ]; then
  echo "Cannot find prior Hadoop configuration directory to run import"
    # And it's non-empty.
  false
fi

old_hbase_dir=`find cluster/lib/ -maxdepth 1 -name "hbase*" -type d`
if [ -z "$old_hbase_dir" ]; then
  echo "Cannot find prior HBase configuration directory to run import"
  false
fi

old_hadoop_conf="$old_hadoop_dir/conf"
old_hbase_conf="$old_hbase_dir/conf"

if [ -f "$old_hadoop_conf/mapred-site.xml" ]; then
  cp "$old_hadoop_conf/mapred-site.xml" "$HADOOP_HOME/conf/"
fi
if [ -f "$old_hadoop_conf/bento-mapred-site.xml" ]; then
  cp "$old_hadoop_conf/bento-mapred-site.xml" "$HADOOP_HOME/conf/"
fi
if [ -f "$old_hadoop_conf/core-site.xml" ]; then
  cp "$old_hadoop_conf/core-site.xml" "$HADOOP_HOME/conf/"
fi
if [ -f "$old_hadoop_conf/bento-core-site.xml" ]; then
  cp "$old_hadoop_conf/bento-core-site.xml" "$HADOOP_HOME/conf/"
fi
if [ -f "$old_hadoop_conf/hdfs-site.xml" ]; then
  cp "$old_hadoop_conf/hdfs-site.xml" "$HADOOP_HOME/conf/"
fi
if [ -f "$old_hadoop_conf/bento-hdfs-site.xml" ]; then
  cp "$old_hadoop_conf/bento-hdfs-site.xml" "$HADOOP_HOME/conf/"
fi

if [ -f "$old_hbase_conf/hbase-site.xml" ]; then
  cp "$old_hbase_conf/hbase-site.xml" "$HBASE_HOME/conf/"
fi
if [ -f "$old_hbase_conf/bento-hbase-site.xml" ]; then
  cp "$old_hbase_conf/bento-hbase-site.xml" "$HBASE_HOME/conf/"
fi

# Move this temporary working BentoBox instance back to where the previous instance
# was located. This completes the upgrade process.
mv "$NEW_BENTO_ROOT"/* "$PREV_BENTO_ROOT"

# After everything is safely moved over, disable the error recovery traps.
trap do_nothing ERR

# BENTO-21: If the codename is different (e.g., buri vs. albacore), rename the final
# destination directory to match the new version, and add a symlink to the old one.
prev_bento_codename=`basename "$PREV_BENTO_ROOT"`
new_bento_codename=`basename "$NEW_BENTO_ROOT"`

if [ "$prev_bento_codename" != "$new_bento_codename" ]; then
  bento_target_parent=`cd "$PREV_BENTO_ROOT"/.. && pwd`
  # Check for errors manually here. We're past the bail-out point in the script
  # and will display the final success message in any case."
  mv_failed=0
  set +e
  pushd "$bento_target_parent"
  if [ `pwd` == "$bento_target_parent" ]; then
    mv "$prev_bento_codename" "$new_bento_codename"
    if [ "$?" != 0 ]; then
      mv_failed=1
    else
      # Symlink the old name to the new target dir.
      ln -s "$new_bento_codename" "$prev_bento_codename"
      if [ "$?" != 0 ]; then
        mv_failed=1
      fi
    fi
    popd
  fi
  if [ "$mv_failed" == "0" ]; then
    echo ""
    echo "Your BentoBox used to be installed in:"
    echo "$bento_target_parent/$prev_bento_codename"
    echo "This upgrade has renamed the BentoBox directory to $new_bento_codename"
    echo "A symlink from the old BentoBox home to the new one has been created."
    echo ""
  else
    echo ""
    echo "WARNING: Your BentoBox is installed in:"
    echo "$bento_target_parent/$prev_bento_codename"
    echo "As part of this upgrade, I tried to move this to $new_bento_codename"
    echo "but that failed. (Maybe you do not own the parent directory?)"
    echo "The upgrade has been successful, but this final rename step did not"
    echo "take place. You may wish to do this manually."
    echo ""
  fi
fi

echo "Your BentoBox has been upgraded!"
echo ""
echo "Your previous BentoBox has been backed up to archive/$archive_dest_base,"
echo "along with a copy of its state dir. If things look like they're working,"
echo "you can remove the cluster/state/ dir from the backup to save space."
echo ""
echo "You should 'source bin/kiji-env.sh' again to use the new environment."
echo "Then type 'bento start' to start a BentoBox cluster."
echo ""

exit 0
