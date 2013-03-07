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
# This script runs a Java program that looks in the file .kiji-bento-upgrade in
# the state directory of the bento cluster included with a Kiji BentoBox for upgrade
# information. If the file exists and contains relevant upgrade information, a reminder
# about the upgrade will be displayed to the user. The frequency with which the user
# is reminded of upgrades can be configured with an argument to the Java program.

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

bin=`dirname $0`

# This script should be in the bin directory of a Kiji BentoBox.
# The Kiji BentoBox lib directory should be one directory above,
# as should the conf dir that contains log4j configuration.
kiji_bento_lib_dir="${bin}/../lib"
kiji_bento_conf_dir="${bin}/../conf"

# We'll look for the update file in bento-cluster's state dir. We'll also log
# any output from the Java tool to a file in that directory.
bento_cluster_state_dir="${bin}/../cluster/state"
$(create_missing_dir "${bento_cluster_state_dir}")
upgrade_informer_log_file="${bento_cluster_state_dir}/bento-upgrade-informer.log"

# Everything in the kiji-bento lib dir should go on the classpath.
tool_classpath="${kiji_bento_lib_dir}/*"

# We'll remind the user every 24 hours of upgrades.
let "reminder_period_millis=24*60*60*1000"

# Run the tool. Only standard error is redirected to a log file, standard out still
# goes to console so we can easily display upgrade messages to the user.
java -cp "${tool_classpath}:${kiji_bento_conf_dir}" org.kiji.bento.box.tools.UpgradeInformerTool \
  "--input-dir=${bento_cluster_state_dir}" \
  "--reminder-period-millis=${reminder_period_millis}" \
  2> "${upgrade_informer_log_file}"

