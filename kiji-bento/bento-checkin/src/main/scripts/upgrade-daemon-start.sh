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
#  Script that launches a daemon that periodically sends check-ins to the
#  BentoBox upgrade server.

function create_missing_dir() {
  dirname="$1"

  # Create the directory identified if it does not exist already.
  if [ ! -z "$dirname" -a ! -d "$dirname" ]; then
    if [ -e "$dirname" ]; then
      echo "Warning: $dirname exists but is not a directory"
      return 1
    fi
    mkdir -p "$dirname"
  fi
}

# This function waits until the upgrade daemon's PID file is present.
function wait_daemon_started() {
  tries=10
  while [ "$tries" -gt 0 ]; do
    tries=$(($tries - 1))
    if [ -e "${daemon_pid_file}" ]; then
      break
    fi
    sleep 2
  done
  if [ -e "${daemon_pid_file}" ]; then
    return 0
  else
    echo "Waiting for upgrade server check-in daemon failed."
    return 1
  fi
}

bin=`dirname $0`

# The url where check-in messages should be sent. Can be overriden by setting the
# environment variable BENTO_CHECKIN_SERVER
BENTO_CHECKIN_SERVER=${BENTO_CHECKIN_SERVER:-"https://updates.kiji.org/api/1.0.0/checkin"}

# This script should be in the bin directory of a bento-cluster packaged in
# a Kiji BentoBox.
kiji_bento_lib_dir="${bin}/../../lib"
kiji_bento_conf_dir="${bin}/../../conf"

# If the file disable-checkin exists in the Kiji BentoBox conf dir, then
# we shouldn't start the checkin server.
if [ -e "${kiji_bento_conf_dir}/disable-checkin" ]; then
  exit 0
fi

# The daemon also needs the bento-cluster's state directory.
bento_cluster_state_dir="${bin}/../state"
daemon_pid_file="${bento_cluster_state_dir}/checkin-daemon.pid"

$(create_missing_dir "${bento_cluster_state_dir}")
upgrade_daemon_log_file="${bento_cluster_state_dir}/bento-upgrade-daemon.log"

# Everything in the kiji-bento lib dir should go on the classpath.
tool_classpath="${kiji_bento_lib_dir}/*"

# We'll check-in with the upgrade server every 12 hours.
let "checkin_period_millis=12*60*60*1000"

# Run the tool in the background.
nohup java -cp "${tool_classpath}:${kiji_bento_conf_dir}" \
  org.kiji.bento.box.tools.UpgradeDaemonTool \
  "--state-dir=${bento_cluster_state_dir}" \
  "--checkin-period-millis=${checkin_period_millis}" \
  "--upgrade-server-url=${BENTO_CHECKIN_SERVER}" \
  &> "${upgrade_daemon_log_file}" < /dev/null &

wait_daemon_started

