#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/*.env

set -x

CMD_LINE_ARGUMENTS="$1"
ROOT_DIR_HDFS_RAW=${3:-"$ROOT_DIR_HDFS_RAW"}
ROOT_DIR_HDFS_STAGED=${3:-"$ROOT_DIR_HDFS_STAGED"}
ROOT_DIR_HDFS_PARTITIONED=${3:-"$ROOT_DIR_HDFS_PARTITIONED"}
ROOT_DIR_HDFS_PROCESSED=${3:-"$ROOT_DIR_HDFS_PROCESSED"}
LIBJARS="$(echo -n $(ls -m $ROOT_DIR/lib/jar/dep/*.jar)|sed 's/, /,/g')"
export HADOOP_CLASSPATH="$(echo -n $(ls -m $ROOT_DIR/lib/jar/dep/*.jar)|sed 's/, /:/g')"

$ROOT_DIR/bin/*-shell-hadoop.sh "\
  jar $ROOT_DIR/lib/jar/*.jar \
  com.cloudera.example.ingest.Ingest \
  -libjars $LIBJARS \
  $CMD_LINE_ARGUMENTS \
  $ROOT_DIR_HDFS_RAW \
  $ROOT_DIR_HDFS_STAGED \
  $ROOT_DIR_HDFS_PARTITIONED" \
  $ROOT_DIR_HDFS_PROCESSED"

$ROOT_DIR/bin/*-data-schema.sh
