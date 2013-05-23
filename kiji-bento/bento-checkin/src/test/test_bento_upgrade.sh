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
# This is a test script to help test the bento upgrade process

# Cleans up itself by killing the python and rails pid (passed as args)
function cleanup() {
  echo "Cleaning up.."
  kill $1
  kill $2
}

# This tests that when you explicitly disable the checkin in the bento conf folder
# and do an upgrade, that it persists the disable-checkin file in both $HOME/.kiji AND
# retains the file in the new bento folder's conf/ directory.
function test1() {
  echo "Test disable checkin persisting"

  touch conf/disable-checkin
  BENTO_CHECKIN_SERVER=http://localhost:3003/api/1.0.0/checkin bento upgrade
  if [ $? != 0 ]; then
    echo "4) Upgrade failed when it should have worked!"
    cleanup $python_pid $rails_pid
    exit 1;
  fi
  ls conf/disable-checkin $HOME/.kiji/.disable_kiji_checkin
  if [ $? != 0 ]; then
    echo "4) Disable checkin didn't persist properly. Return Code=$?"
    cleanup $python_pid $rails_pid
    exit 1;
  fi
}

#Setup
curr_dir=$(pwd)

bento_target=$1
checkin_server=$2
if [ "$bento_target" == "" ]; then
  echo "Please specify the target/ folder for where kiji-bento compiles"
  exit 1
fi

if [ "$checkin_server" == "" ]; then
  echo "Please specify the target/ folder for where the checkin server resides."
  exit 1
fi

echo "Setting up python http server"

cd $bento_target
python -m SimpleHTTPServer &
python_pid=$!

echo "Setting up checkin server"
cd $checkin_server

rails server -d
rails_pid=$!

echo "Python running ${python_pid} Rails running ${rails_pid}" > ${curr_dir}/test_pids.txt

cd ${bento_target}/kiji-bento*-release/kiji-bento-*
source bin/kiji-env.sh

# Run test methods

test1

# teardown
cleanup $python_pid $rails_pid
cd $curr_dir
rm -f ${curr_dir}/test_pids.txt