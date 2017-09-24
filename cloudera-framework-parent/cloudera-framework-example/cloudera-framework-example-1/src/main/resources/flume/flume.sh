#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "$BASH_SOURCE" )" && pwd )/../..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e
 
export FLUME_AGENT_CONFIG=$(echo "$FLUME_AGENT_CONFIG" | \
	sed -e "s|\$RECORD_FORMAT|$RECORD_FORMAT|g" \
)
