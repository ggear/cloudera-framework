#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/../../..

source $ROOT_DIR/bin/*.env

set -x

CMD_LINE_ARGUMENTS="$1"
DROP_SCHEMA=${2:-true}

$ROOT_DIR/bin/init/post/cloudera-framework-example-schema.sh "" "$DROP_SCHEMA"
