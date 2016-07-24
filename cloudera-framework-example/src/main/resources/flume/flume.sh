#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/../..

source $ROOT_DIR/bin/*.env

set -x -e
 
export FLUME_AGENT_CONFIG=$(echo "$FLUME_AGENT_CONFIG" | \
	sed -e "s|\$RECORD_FORMAT|$RECORD_FORMAT|g" \
)
