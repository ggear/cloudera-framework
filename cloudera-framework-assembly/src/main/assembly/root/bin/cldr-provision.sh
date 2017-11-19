#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "$BASH_SOURCE" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

DELETE_CLUSTER=${1:-"false"}
MANAGER_SERVER_USER=${2:-"admin"}
MANAGER_SERVER_PWORD=${3:-"admin"}

if [ "$CLUSTER_PROVISION" = "altus" ]; then
  $ROOT_DIR/bin/cldr-provision-altus.sh "$DELETE_CLUSTER"
elif [ "$CLUSTER_PROVISION" = "director" ]; then
  $ROOT_DIR/bin/cldr-provision-director.sh "$DELETE_CLUSTER"
elif [ "$CLUSTER_PROVISION" = "manual" ]; then
  $ROOT_DIR/lib/manager/python/environment.py \
    --host "$MANAGER_SERVER_HOST" \
    --port "$MANAGER_SERVER_PORT" \
    --user "$MANAGER_SERVER_USER" \
    --password "$MANAGER_SERVER_PWORD" > $ROOT_DIR/cfg/cluster.env
fi
