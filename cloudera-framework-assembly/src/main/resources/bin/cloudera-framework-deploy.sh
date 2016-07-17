#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/*.env

set -x

if [ -f $ROOT_DIR/lib/parcel/parcel.env ]; then
  $ROOT_DIR/bin/cloudera-framework-parcel.py \
	--host $MANAGER_SERVER_HOST \
	--parcel_name $PARCEL_NAME \
	--parcel_version $PARCEL_VERSION \
	--parcel_repo $PARCEL_REPO/$PARCEL_VERSION_SHORT \
	--init_pre_dir $ROOT_DIR/bin/init/pre \
	--init_post_dir $ROOT_DIR/bin/init/post \
	|| { exit 1; }
fi
	