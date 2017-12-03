#!/bin/bash
#
# Driver script
#
# This example launches a Spark2 job based on the packaged example
# "${package}.Driver". Note that the ../cfg/${artifactId}.env
# defines how to connect, and provision in the case of Cloudera Altus and
# Cloudera Director) to the cluster.
#

export ROOT_DIR=$( cd "$( dirname "$BASH_SOURCE" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

WAIT_TASK=${1:-"false"}
DELETE_CLUSTER=${2:-"false"}

[[ "$DELETE_CLUSTER" = "true" ]] && WAIT_TASK="true"

$ROOT_DIR/bin/cldr-provision.sh "$WAIT_TASK"

$ROOT_DIR/bin/cldr-shell-spark2.sh \
  "$WAIT_TASK" \
  "${altusCluster}-job" \
  "${package}.Driver" \
  "$STORAGE_URL/data/workload/" \
  "--num-executors ""$SPARK_EXEC_NUM"" --executor-cores ""$SPARK_EXEC_CORES"" --executor-memory ""$SPARK_EXEC_MEMORY""" \
  "$STORAGE_URL/tmp/jar/"

[[ "$DELETE_CLUSTER" = "true" ]] && $ROOT_DIR/bin/cldr-provision.sh "true" "true"
