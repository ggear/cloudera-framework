#!/bin/bash

ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

$ROOT_DIR/cloudera-framework-initialise.sh

export CDH_HADOOP_HOME=$PARCELS_ROOT/$CDH_DIRNAME/lib/hadoop

export HIVE_AUX_JARS_PATH=$ROOT_DIR/../lib

