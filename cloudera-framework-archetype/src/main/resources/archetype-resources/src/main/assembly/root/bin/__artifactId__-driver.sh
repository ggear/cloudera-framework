#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/*.env

set -x

CMD_LINE_ARGUMENTS="$1"
ROOT_DIR_HDFS=${2:-"$ROOT_DIR_HDFS"}

$ROOT_DIR/bin/cloudera-framework-hadoop.sh "\
  jar $ROOT_DIR/lib/jar/*.jar \
  com.cloudera.MyDriver \
  -libjars $LIBJARS \
  $CMD_LINE_ARGUMENTS \
  $ROOT_DIR_HDFS
