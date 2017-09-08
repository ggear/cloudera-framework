#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

$ROOT_DIR/bin/cldr-shell-hadoop.sh "\
  jar $ROOT_DIR/lib/jar/*.jar \
  com.cloudera.framework.example.one.process.stage.Process \
  -libjars $LIBJARS \
  $ROOT_DIR_HDFS_RAW \
  $ROOT_DIR_HDFS_STAGED \
  $ROOT_DIR_HDFS_PARTITIONED \
  $ROOT_DIR_HDFS_CLEANSED"

$ROOT_DIR/bin/cldr-data-schema.sh
