#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/*.env

set -x -e

if $USER_ADMIN; then
  USER_APP=$USER_ADMIN_HDFS
fi

sudo su $USER_APP -c "export HADOOP_CLASSPATH=$HADOOP_CLASSPATH; hadoop --config $HADOOP_CONF_DIR $@"
