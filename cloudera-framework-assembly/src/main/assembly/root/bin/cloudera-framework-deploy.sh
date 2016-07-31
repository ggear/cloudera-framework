#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/*.env

set -x -e

MANAGER_SERVER_USER=${1:-"admin"}
MANAGER_SERVER_PWORD=${2:-"admin"}

if [ -f $ROOT_DIR/lib/parcel/parcel.env ]; then
  id -u $PARCEL_NAMESPACE &>/dev/null || sudo useradd $PARCEL_NAMESPACE 
  $ROOT_DIR/lib/manager/python/deploy.py \
    --user $MANAGER_SERVER_USER \
    --password $MANAGER_SERVER_PWORD \
    --man_host $MANAGER_SERVER_HOST \
    --parcel_name $PARCEL_NAME \
    --parcel_version $PARCEL_VERSION \
    --parcel_repo $PARCEL_REPO/$PARCEL_VERSION_SHORT \
    --init_pre_dir $ROOT_DIR/bin/init/pre \
    --init_post_dir $ROOT_DIR/bin/init/post
fi
