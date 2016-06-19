#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/../../..

source $ROOT_DIR/bin/*.env

set -x

CMD_LINE_ARGUMENTS="$1"

echo "Do nothing for the time being!"
