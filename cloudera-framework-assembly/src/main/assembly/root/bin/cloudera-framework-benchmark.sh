#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/*.env

set -x

MANAGER_SERVER_USER=${1:-"admin"}
MANAGER_SERVER_PWORD=${2:-"admin"}
SLEEP_PERIOD_S=${3:-"10"}
STREAM_PERIOD_S=${4:-"60"}

function benchmark_failed {
  set +x
  echo "" && echo "-------------------------------------------------------------------------------"
  echo "Benchmark pipeline failed, see above"
  echo "-------------------------------------------------------------------------------" && echo ""
  exit 1
}

function benchmark_succeeded {
  set +x
  echo "" && echo "-------------------------------------------------------------------------------"
  echo "Benchmark pipeline succeeded in [$DURATION_S] seconds"
  echo "-------------------------------------------------------------------------------" && echo ""
  exit 1
}

{ $ROOT_DIR/bin/cloudera-framework-deploy.sh; } || { benchmark_failed; }

START_S=$(date +%s)
{ $ROOT_DIR/bin/cloudera-framework-stream.sh true || benchmark_failed; } && { sleep $STREAM_PERIOD_S; }
{ $ROOT_DIR/bin/cloudera-framework-stream.sh false && sleep $SLEEP_PERIOD_S; } || { benchmark_failed; }
[ $(find $ROOT_DIR/bin -maxdepth 1 -type f -name "*-process.sh" | wc -l) -gt 1 ] && { echo "Error - multiple process scripts found" && benchmark_failed; }
[ $(find $ROOT_DIR/bin -maxdepth 1 -type f -name "*-process.sh" | wc -l) -eq 1 ] && { $ROOT_DIR/bin/*-process.sh && sleep $SLEEP_PERIOD_S || benchmark_failed; }
{ $ROOT_DIR/bin/cloudera-framework-query.sh && sleep $SLEEP_PERIOD_S; } || { benchmark_failed; }
FINISH_S=$(date +%s)

DURATION_S=$((FINISH_S-START_S))
{ $ROOT_DIR/lib/manager/python/benchmark.py \
  --host $MANAGER_SERVER_HOST \
  --user $MANAGER_SERVER_USER \
  --password $MANAGER_SERVER_PWORD; } || { benchmark_failed; }

benchmark_succeeded
