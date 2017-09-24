#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "$BASH_SOURCE" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

if [ "$USER_ADMIN" = true ]; then
  USER_APP=$USER_ADMIN_IMPALA
  if [ "$DATABASE_DEFAULT" = true ]; then
    DATABASE_APP=default
  fi
fi

impala-shell \
	-i $IMPALA_IMPALAD_HOST:$IMPALA_IMPALAD_PORT \
	-u $USER_APP \
	-d $DATABASE_APP \
	"$@"
