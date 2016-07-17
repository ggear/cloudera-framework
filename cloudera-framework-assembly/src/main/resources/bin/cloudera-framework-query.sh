#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/*.env

set -x

CMD_LINE_ARGUMENTS="$1"
ROOT_DIR_PYTHON=${2:-"$ROOT_DIR/lib/python"}
ROOT_DIR_HIVE_QUERY=${3:-"$ROOT_DIR/lib/hive/query"}
ROOT_DIR_IMPALA_QUERY=${4:-"$ROOT_DIR/lib/impala/query"}

for SCRIPT in $(ls $ROOT_DIR_PYTHON 2> /dev/null); do
  $ROOT_DIR_PYTHON/$SCRIPT
done
for SCRIPT in $(ls $ROOT_DIR_HIVE_QUERY 2> /dev/null); do
  if [ ${SCRIPT: -3} == ".sh" ]; then
    $ROOT_DIR_HIVE_QUERY/$SCRIPT
  else
    $ROOT_DIR/bin/cloudera-framework-hive.sh -f $ROOT_DIR_HIVE_QUERY/$SCRIPT
  fi
done
for SCRIPT in $(ls $ROOT_DIR_IMPALA_QUERY 2> /dev/null); do
  if [ ${SCRIPT: -3} == ".sh" ]; then
    $ROOT_DIR_IMPALA_QUERY/$SCRIPT
  else
    $ROOT_DIR/bin/cloudera-framework-impala.sh -f $ROOT_DIR_IMPALA_QUERY/$SCRIPT
  fi
done
