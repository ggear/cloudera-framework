#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/*.env

set -x

beeline \
	-u "jdbc:hive2://$HIVE_HIVESERVER2_HOST:$HIVE_HIVESERVER2_PORT/$DATABASE_APP;user=$USER_ADMIN" \
	-n $USER_ADMIN \
	--hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat \
	"$@"
