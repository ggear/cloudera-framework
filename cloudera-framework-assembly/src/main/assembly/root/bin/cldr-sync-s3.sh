#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "$BASH_SOURCE" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

BUCKET_FROM=${1:-"s3a://mybucket-from/"}
BUCKET_TO=${2:-"s3a://mybucket-to/"}
NOT_DRYRUN=${3:-"false"}
NOT_DELETE=${4:-"true"}

[[ "$NOT_DRYRUN" = "true" ]] && S3_DRYRUN="" || S3_DRYRUN="--dryrun"
[[ "$NOT_DELETE" = "true" ]] && S3_DELETE="" || S3_DELETE="--delete"

aws configure set default.s3.max_concurrent_requests 50
aws configure set default.s3.max_queue_size 500

aws s3 sync "${BUCKET_FROM/s3a:\/\//s3://}" "${BUCKET_TO/s3a:\/\//s3://}" --size-only $S3_DELETE $S3_DRYRUN
