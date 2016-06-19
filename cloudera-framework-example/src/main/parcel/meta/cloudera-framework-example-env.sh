#!/bin/bash

ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

$ROOT_DIR/cloudera-framework-initialise.sh

export HIVE_AUX_JARS_PATH="$ROOT_DIR/lib/jar"
export HADOOP_CLASSPATH="$HADOOP_CLASSPATH":"$(echo -n $(ls -m $ROOT_DIR/lib/jar/*.jar)|sed 's/, /:/g')"
