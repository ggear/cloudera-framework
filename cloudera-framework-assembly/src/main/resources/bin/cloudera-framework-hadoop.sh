#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/*.env

set -x

sudo su $USER_ADMIN -c "export HADOOP_CLASSPATH=$HADOOP_CLASSPATH; hadoop --config $HADOOP_CONF_DIR $@"
