#!/bin/bash
###############################################################################
#
# Driver script
#
###############################################################################

export ROOT_DIR=$( cd "$( dirname "$BASH_SOURCE" )" && pwd )/..

source $ROOT_DIR/bin/*.env

set -x -e

$ROOT_DIR/bin/cloudera-framework-spark.sh "\
  jar $ROOT_DIR/lib/jar/*.jar \
  ${package}.Driver \
  -libjars $LIBJARS \
  $ROOT_DIR_HDFS
