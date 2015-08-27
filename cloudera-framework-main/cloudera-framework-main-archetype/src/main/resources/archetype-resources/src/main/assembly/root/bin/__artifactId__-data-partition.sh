#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/*.env

set -x

CMD_LINE_ARGUMENTS="$1"
ROOT_DIR_HDFS_STAGED_CANONICAL=${2:-"$ROOT_DIR_HDFS_STAGED_CANONICAL"}
ROOT_DIR_HDFS_PARTITIONED=${3:-"$ROOT_DIR_HDFS_PARTITIONED"}
LIBJARS="$(echo -n $(ls -m $ROOT_DIR/lib/jar/dep/*.jar)|sed 's/, /,/g')"
export HADOOP_CLASSPATH="$(echo -n $(ls -m $ROOT_DIR/lib/jar/dep/*.jar)|sed 's/, /:/g')"

$ROOT_DIR/bin/*-shell-hadoop.sh "\
  jar $ROOT_DIR/lib/jar/*.jar \
  com.cloudera.example.partition.Partition \
  -libjars $LIBJARS \
  $CMD_LINE_ARGUMENTS \
  $ROOT_DIR_HDFS_STAGED_CANONICAL \
  $ROOT_DIR_HDFS_PARTITIONED"