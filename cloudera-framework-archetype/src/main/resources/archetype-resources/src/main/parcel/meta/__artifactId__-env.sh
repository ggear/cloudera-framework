#!/bin/bash

PARCEL_DIR=$( cd "$( dirname "$BASH_SOURCE" )" && pwd )/..

$PARCEL_DIR/meta/cloudera-framework-initialise.sh

export HIVE_AUX_JARS_PATH="$PARCEL_DIR/lib/jar"
export HADOOP_CLASSPATH="$HADOOP_CLASSPATH":"$(echo -n $(ls -m $PARCEL_DIR/lib/jar/*.jar)|sed 's/, /:/g')"
