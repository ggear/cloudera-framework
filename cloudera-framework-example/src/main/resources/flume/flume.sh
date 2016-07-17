#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/../..

source $ROOT_DIR/bin/*.env

set -x

CMD_LINE_ARGUMENTS="$1"
ROOT_DIR_HDFS_STAGED_CANONICAL=${14:-"$ROOT_DIR_HDFS_STAGED_CANONICAL"}
RECORD_FORMAT=${15:-"xml"}
 
export FLUME_AGENT_CONFIG=$(echo "$FLUME_AGENT_CONFIG" | \
	sed -e "s|\$RECORD_FORMAT|$RECORD_FORMAT|g" \
)
