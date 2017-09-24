#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "$BASH_SOURCE" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

if [ "$USER_ADMIN" = true ]; then
  USER_APP=$USER_ADMIN_HDFS
fi

# TODO: Resolve lack of sudo and local user on CDSW nodes
#sudo su $USER_APP -c "export HADOOP_CLASSPATH=$HADOOP_CLASSPATH; spark-submit $@"

bash -c "export HADOOP_CLASSPATH=$HADOOP_CLASSPATH; spark2-submit $@"
