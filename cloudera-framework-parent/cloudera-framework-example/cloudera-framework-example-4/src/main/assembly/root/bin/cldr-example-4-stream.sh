#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "$BASH_SOURCE" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

WAIT_TASK=${1:-"true"}
DELETE_CLUSTER=${6:-"false"}

[[ "$DELETE_CLUSTER" = "true" ]] && WAIT_TASK="true"

$ROOT_DIR/bin/cldr-provision.sh "$WAIT_TASK"

$ROOT_DIR/bin/cldr-shell-spark2.sh \
  "$WAIT_TASK" \
  "cldr-example-4" \
  "com.cloudera.framework.example.four.Stream" \
  "$S3_URL/cldr-example-4/csv/ $S3_URL/cldr-example-4/parquet/" \
  "--num-executors ""$SPARK_EXEC_NUM"" --executor-cores ""$SPARK_EXEC_CORES"" --executor-memory ""$SPARK_EXEC_MEMORY""" \
  "$S3_URL/cldr-example-4/jar/"

[[ "$DELETE_CLUSTER" = "true" ]] && $ROOT_DIR/bin/cldr-provision.sh "true" "true"
