#!/bin/bash

# Run a series of gets and posts.

# Get directory from which the script is executing.
DIR=$( cd $( dirname $0 ) && pwd )

# Check if GET or POST is request.
if [ "$1" = "GET" ]; then
  OPER="gets"
elif [ "$1" = "POST" ]; then
  OPER="posts"
elif [ "$1" = "FRESHEN" ]; then
  OPER="freshgets"
else
  echo "Usage:"
  echo "  sh test-suite.sh GET http://domain:1234 instance table"
  echo "  sh test-suite.sh POST http://domain:1234 instance table"
  echo "  sh test-suite.sh FRESHEN http://domain:1234 instance table"
  exit 0
fi

# Run test.
for THREADS in 10 20 30 40 50 60 70 80 90 100 200 300 400 500
do
  echo Performing ${OPER} with $THREADS users...
  jmeter -n -t ${DIR}/../test-plans/${OPER}-test-plan.jmx -l ${DIR}/../logs/${OPER}_${THREADS}_threads.csv -Jthreads=$THREADS -Jrampup=10 -Jloop=100 -Jdomain=$2 -Jinstance=$3 -Jtable=$4
done
