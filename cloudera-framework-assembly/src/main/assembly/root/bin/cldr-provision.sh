#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

MANAGER_SERVER_USER=${1:-"admin"}
MANAGER_SERVER_PWORD=${2:-"admin"}

$ROOT_DIR/lib/manager/python/environment.py \
  --host "$MANAGER_SERVER_HOST" \
  --port "$MANAGER_SERVER_PORT" \
  --user "$MANAGER_SERVER_USER" \
  --password "$MANAGER_SERVER_PWORD" > $ROOT_DIR/cfg/cluster.env

cat $ROOT_DIR/cfg/cluster.env
