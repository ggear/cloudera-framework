#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/*.env

set -x -e

if $USER_ADMIN; then
  USER_APP=$USER_ADMIN_IMPALA
  DATABASE_APP=default
fi

impala-shell \
	-i $IMPALA_IMPALAD_HOST:$IMPALA_IMPALAD_PORT \
	-u $USER_APP \
	-d $DATABASE_APP \
	"$@"
