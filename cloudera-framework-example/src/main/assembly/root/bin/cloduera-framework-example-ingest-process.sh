#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/*.env

set -x

CMD_LINE_ARGUMENTS="$1"
ROOT_DIR_HDFS_PARTITIONED_CANONICAL=${2:-"$ROOT_DIR_HDFS_PARTITIONED_CANONICAL"}
ROOT_DIR_HDFS_PROCESSED=${3:-"$ROOT_DIR_HDFS_PROCESSED"}

$ROOT_DIR/lib/bin/cloudera-framework-hadoop.sh "\
  jar $ROOT_DIR/lib/jar/*.jar \
  com.cloudera.framework.example.ingest.Process \
  -libjars $LIBJARS \
  $CMD_LINE_ARGUMENTS \
  $ROOT_DIR_HDFS_PARTITIONED_CANONICAL \
  $ROOT_DIR_HDFS_PROCESSED"
