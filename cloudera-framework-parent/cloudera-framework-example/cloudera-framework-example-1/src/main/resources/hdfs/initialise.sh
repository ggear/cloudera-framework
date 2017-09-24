#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "$BASH_SOURCE" )" && pwd )/../..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

$ROOT_DIR/bin/cldr-shell-hadoop.sh "fs -mkdir -p /user/$USER_APP"
$ROOT_DIR/bin/cldr-shell-hadoop.sh "fs -chown -R $USER_APP /user/$USER_APP"

$ROOT_DIR/bin/cldr-shell-hadoop.sh "fs -mkdir -p $ROOT_DIR_HDFS_STAGED_CANONICAL"
$ROOT_DIR/bin/cldr-shell-hadoop.sh "fs -chmod -R 777 $ROOT_DIR_HDFS_STAGED_CANONICAL"
