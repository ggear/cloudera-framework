#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/../..

source $ROOT_DIR/bin/*.env

set -x

impala-shell \
	-i $IMPALA_IMPALAD_HOST:$IMPALA_IMPALAD_PORT \
	-u $USER_ADMIN \
	-d $NAME_SPACE_DATABASE \
	"$@"
