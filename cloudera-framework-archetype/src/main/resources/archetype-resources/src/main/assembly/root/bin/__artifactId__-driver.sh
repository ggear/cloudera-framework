#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/*.env

set -x

CMD_LINE_ARGUMENTS="$1"

$ROOT_DIR/lib/bin/cloudera-framework-hadoop.sh "\
  jar $ROOT_DIR/lib/jar/*.jar \
  com.my.company.MyDriver \
  -libjars $LIBJARS