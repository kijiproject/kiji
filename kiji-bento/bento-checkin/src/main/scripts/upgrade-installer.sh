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
# This script runs a Java program that queries the upgrade server for new version
# availability. If a new Bento version is available, it downloads and installs it
# in-place in this Bento instance.

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

bin=`dirname $0`
bin=`cd "$bin" && pwd`
bin=`resolve_symlink "$bin"`

# Determine where the root of this bento box is to upgrade.
# This script should be in cluster/bin/.
bento_root="$bin/../../"
bento_root=`resolve_symlink "$bento_root"`

# The url where check-in messages should be sent. Can be overriden by setting the
# environment variable BENTO_CHECKIN_SERVER
BENTO_CHECKIN_SERVER=${BENTO_CHECKIN_SERVER:-"https://updates.kiji.org/api/1.0.0/checkin"}

# The temp directory where a new download should be unpacked. Can be overridden
# by setting BENTO_TMP_DIR in the environment. For best performance, should be
# on a local filesystem, and preferably the same physical filesystem as this
# destination directory.
BENTO_TMP_DIR=${BENTO_TMP_DIR:-"/tmp"}

# This script should be in the cluster/bin directory of a Kiji BentoBox.
# The Kiji BentoBox lib directory should be one directory above,
# as should the conf dir that contains log4j configuration.
kiji_bento_lib_dir="${bento_root}/lib"
kiji_bento_conf_dir="${bento_root}/conf"

# Everything in the kiji-bento lib dir should go on the classpath.
tool_classpath="${kiji_bento_lib_dir}/*"

# Run the tool. stdout and stderr both print to the screen.
java \
  -cp "${tool_classpath}:${kiji_bento_conf_dir}" org.kiji.bento.box.tools.UpgradeInstallTool \
  --bento-root="$bento_root" \
  --upgrade-server-url="$BENTO_CHECKIN_SERVER" \
  --tmp-dir="$BENTO_TMP_DIR" "$@"


