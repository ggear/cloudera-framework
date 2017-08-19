#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/../..

source $ROOT_DIR/bin/*.env

set -x -e

$ROOT_DIR/bin/cloudera-framework-hadoop.sh "fs -mkdir -p /user/$USER_APP"
$ROOT_DIR/bin/cloudera-framework-hadoop.sh "fs -chown -R $USER_APP /user/$USER_APP"

$ROOT_DIR/bin/cloudera-framework-hadoop.sh "fs -mkdir -p $ROOT_DIR_HDFS_STAGED_CANONICAL"
$ROOT_DIR/bin/cloudera-framework-hadoop.sh "fs -chmod -R 777 $ROOT_DIR_HDFS_STAGED_CANONICAL"
