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
#  This script kills an upgrade server check-in daemon that is running.


# Kill a running checkin daemon process from its pid file.
bin=`dirname $0`
daemon_pid_file="${bin}/../state/checkin-daemon.pid"

if [ ! -f "${daemon_pid_file}" ]; then
  exit 1
fi
PID=`cat ${daemon_pid_file}`
if [ -z "$PID" ]; then
  exit 1
fi
#echo "Stopping upgrade check-in daemon..."
kill $PID
ret=$?
if [ "$ret" != "0" ]; then
  echo "Error killing process with pid $PID"
  exit 1
fi
#echo "Waiting for shutdown..."
tries=15
while [ "$tries" -gt 0 ]; do
  tries=$(($tries - 1))
  ps -e -o pid,args | grep "^$PID" 2>&1 > /dev/null
  ret=$?
  if [ "$ret" != 0 ]; then
    break
  fi
  sleep 2
done
ps e | grep "^$PID " 2>&1 > /dev/null
ret=$?
if [ "$ret" == 0 ]; then
  echo "Kill signal sent but waiting for upgrade check-in daemon to stop timed out after 30 seconds!."
  exit 1
fi
# upgrade check-in daemon will delete its own pid file.
#echo "upgrade check-in daemon shutdown."

