#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/../..

source $ROOT_DIR/bin/*.env
source $ROOT_DIR/lib/parcel/parcel.env
set -x

$ROOT_DIR/lib/bin/cloudera-framework-parcel.py \
	--host $MANAGER_SERVER_HOST \
	--parcel_name $PARCEL_NAME \
	--parcel_version $PARCEL_VERSION \
	--parcel_repo $PARCEL_REPO/$PARCEL_VERSION_SHORT

echo $?
