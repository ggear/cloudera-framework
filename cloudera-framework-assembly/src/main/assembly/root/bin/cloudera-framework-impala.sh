#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

if $USER_ADMIN; then
  USER_APP=$USER_ADMIN_IMPALA
  if $DATABASE_DEFAULT; then
    DATABASE_APP=default
  fi
fi

impala-shell \
	-i $IMPALA_IMPALAD_HOST:$IMPALA_IMPALAD_PORT \
	-u $USER_APP \
	-d $DATABASE_APP \
	"$@"
