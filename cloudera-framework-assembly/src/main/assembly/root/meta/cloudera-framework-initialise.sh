#!/bin/bash

ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

source $ROOT_DIR/parcel.env

PARCEL_LOCAL=$ROOT_DIR/../../.flood/$PARCEL_FILE/$PARCEL_FILE
PARCEL_REMOTE=$PARCEL_REPO/$PARCEL_VERSION_SHORT/$PARCEL_FILE
SHA1_LOCAL=$(sha1sum  $PARCEL_LOCAL | cut -c1-40 | tr '[:upper:]' '[:lower:]')
SHA1_REMOTE=$(curl --silent $PARCEL_REMOTE.sha1 | tr '[:upper:]' '[:lower:]')
if [ "$SHA1_LOCAL" != "$SHA1_REMOTE" ]; then
	echo ""
	echo ""
	echo "---------------------------------------------------------------------------------------------------------------------------------"
	echo "---------------------------------------------------------------------------------------------------------------------------------"
	echo ""
	echo "ERROR: Parcel checksum failed, local parcel different to that in the repo, parcel idempotency violated, using local parcel, but likely not the version intended"
	echo ""
	echo "LOCAL FILE: $SHA1_LOCAL $PARCEL_LOCAL"
	echo "LOCAL FILE: $SHA1_REMOTE $PARCEL_REMOTE"
	echo ""
	echo "---------------------------------------------------------------------------------------------------------------------------------"	
	echo "---------------------------------------------------------------------------------------------------------------------------------"
	echo ""
	echo ""
fi
