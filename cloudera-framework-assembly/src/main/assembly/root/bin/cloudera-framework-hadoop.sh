#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

if $USER_ADMIN; then
  USER_APP=$USER_ADMIN_HDFS
fi

sudo su $USER_APP -c "export HADOOP_CLASSPATH=$HADOOP_CLASSPATH; hadoop --config $HADOOP_CONF_DIR $@"
