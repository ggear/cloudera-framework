#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/../../..

source $ROOT_DIR/bin/*.env

set -x

echo "Pre cluster start parcel init script"
