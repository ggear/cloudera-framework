#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/*.env

set -x

$ROOT_DIR/bin/*-schema.sh
$ROOT_DIR/bin/*-cleanse.sh
