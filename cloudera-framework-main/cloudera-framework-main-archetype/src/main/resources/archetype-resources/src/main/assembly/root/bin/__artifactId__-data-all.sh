#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/*.env

set -x

$ROOT_DIR/bin/*-data-schema.sh
$ROOT_DIR/bin/*-data-stream.sh
$ROOT_DIR/bin/*-data-stage.sh
$ROOT_DIR/bin/*-data-partition.sh
$ROOT_DIR/bin/*-data-process.sh
