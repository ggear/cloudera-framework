#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

if $USER_ADMIN; then
  USER_APP=$USER_ADMIN_HIVE
  if $DATABASE_DEFAULT; then
    DATABASE_APP=default
  fi
fi

beeline \
	-u "jdbc:hive2://$HIVE_HIVESERVER2_HOST:$HIVE_HIVESERVER2_PORT/$DATABASE_APP;user=$USER_APP" \
	-n $USER_APP \
	--hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat \
	"$@"
