#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "$BASH_SOURCE" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

$ROOT_DIR/bin/cldr-shell-hadoop.sh "\
  jar $ROOT_DIR/lib/jar/*.jar \
  com.cloudera.framework.example.one.process.stage.Stage \
  -libjars $LIBJARS \
  $ROOT_DIR_HDFS_RAW_CANONICAL \
  $ROOT_DIR_HDFS_STAGED"
