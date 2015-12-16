#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/*.env

set -x

CMD_LINE_ARGUMENTS="$1"
ROOT_DIR_HDFS_RAW_CANONICAL=${2:-"$ROOT_DIR_HDFS_RAW_CANONICAL"}
ROOT_DIR_HDFS_STAGED=${3:-"$ROOT_DIR_HDFS_STAGED"}

$ROOT_DIR/bin/*-shell-hadoop.sh "\
  jar $ROOT_DIR/lib/jar/*.jar \
  com.cloudera.example.ingest.Stage \
  -libjars $LIBJARS \
  $CMD_LINE_ARGUMENTS \
  $ROOT_DIR_HDFS_RAW_CANONICAL \
  $ROOT_DIR_HDFS_STAGED"
