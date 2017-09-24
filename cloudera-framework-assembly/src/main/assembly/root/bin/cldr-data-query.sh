#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "$BASH_SOURCE" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

ROOT_DIR_PYTHON=${1:-"$ROOT_DIR/lib/python"}
ROOT_DIR_HIVE_QUERY=${2:-"$ROOT_DIR/lib/hive/query"}
ROOT_DIR_IMPALA_QUERY=${3:-"$ROOT_DIR/lib/impala/query"}

for SCRIPT in $(find $ROOT_DIR_PYTHON -maxdepth 1 -type f 2> /dev/null); do
  $SCRIPT
done
for SCRIPT in $(find $ROOT_DIR_HIVE_QUERY -maxdepth 1 -type f 2> /dev/null); do
  if [ ${SCRIPT: -3} == ".sh" ]; then
    $SCRIPT
  else
    $ROOT_DIR/bin/cldr-shell-hive.sh -f $SCRIPT
  fi
done
for SCRIPT in $(find $ROOT_DIR_IMPALA_QUERY -maxdepth 1 -type f 2> /dev/null); do
  if [ ${SCRIPT: -3} == ".sh" ]; then
    $SCRIPT
  else
    $ROOT_DIR/bin/cldr-shell-impala.sh -f $SCRIPT
  fi
done
