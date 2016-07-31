#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/*.env

set -x -e

DROP_SCHEMA=${1:-false}
DROP_DIR=${2:-false}
ROOT_DIR_HIVE_SCHEMA=${3:-"$ROOT_DIR/lib/hive/schema"}
ROOT_DIR_HIVE_REFRESH=${4:-"$ROOT_DIR/lib/hive/refresh"}
ROOT_DIR_IMPALA_SCHEMA=${5:-"$ROOT_DIR/lib/impala/schema"}
ROOT_DIR_IMPALA_REFRESH=${6:-"$ROOT_DIR/lib/impala/refresh"}
ROOT_DIR_HDFS_INIT=${7:-"$ROOT_DIR/lib/hdfs"}

export USER_ADMIN=true
export DATABASE_DEFAULT=true

if $DROP_SCHEMA; then
  if [ $($ROOT_DIR/bin/cloudera-framework-impala.sh -d default -q "SHOW DATABASES" 2> /dev/null| grep $DATABASE_APP|wc -l) -gt 0 ]; then
    $ROOT_DIR/bin/cloudera-framework-impala.sh -d default -q "DROP DATABASE IF EXISTS $DATABASE_APP CASCADE"
  fi
  if [ $($ROOT_DIR/bin/cloudera-framework-impala.sh -d default -q "SHOW ROLES" 2> /dev/null| grep $USER_APP|wc -l) -gt 0 ]; then
    $ROOT_DIR/bin/cloudera-framework-impala.sh -d default -q "DROP ROLE $USER_APP;"
  fi
fi

if $DROP_DIR; then
  $ROOT_DIR/bin/cloudera-framework-hadoop.sh "fs -rm -r -f $ROOT_DIR_HDFS"
fi

if $DROP_SCHEMA || $DROP_DIR; then
  exit 0
fi

if [ $($ROOT_DIR/bin/cloudera-framework-impala.sh -d default -q "SHOW ROLES" 2> /dev/null| grep $USER_APP|wc -l) -eq 0 ]; then
  $ROOT_DIR/bin/cloudera-framework-hadoop.sh "fs -mkdir -p $ROOT_DIR_HDFS"
  $ROOT_DIR/bin/cloudera-framework-hadoop.sh "fs -chown $USER_ADMIN_HIVE $ROOT_DIR_HDFS"
  if [ $($ROOT_DIR/bin/cloudera-framework-hive.sh -e "SHOW ROLES" 2> /dev/null| grep $USER_ADMIN_HIVE|wc -l) -eq 0 ]; then
    $ROOT_DIR/bin/cloudera-framework-hive.sh -e "CREATE ROLE $USER_ADMIN_HIVE;"
  fi
  $ROOT_DIR/bin/cloudera-framework-hive.sh \
    --hivevar user.app=$USER_APP \
    --hivevar user.admin=$USER_ADMIN_HIVE \
    --hivevar user.server=$USER_SERVER \
    --hivevar database.name=$DATABASE_APP \
    --hivevar database.location=$ROOT_DIR_HDFS \
    -f $ROOT_DIR/lib/hive/database/create.ddl
  until $ROOT_DIR/bin/cloudera-framework-impala.sh -r -q "USE $DATABASE_APP; SHOW TABLES" 2> /dev/null; do
  	echo "Sleeping while waiting admin role to sync ... "
    sleep 5
  done
  export DATABASE_DEFAULT=false
  for SCRIPT in $(find $ROOT_DIR_HIVE_SCHEMA -maxdepth 1 -type f 2> /dev/null); do
    if [ ${SCRIPT: -3} == ".sh" ]; then
      $SCRIPT
    else
      $ROOT_DIR/bin/cloudera-framework-hive.sh -f $SCRIPT
    fi
  done
  for SCRIPT in $(find $ROOT_DIR_IMPALA_SCHEMA -maxdepth 1 -type f 2> /dev/null); do
    if [ ${SCRIPT: -3} == ".sh" ]; then
      $SCRIPT
    else
      $ROOT_DIR/bin/cloudera-framework-impala.sh -f $SCRIPT
    fi
  done
  $ROOT_DIR/bin/cloudera-framework-impala.sh -r -q "SHOW TABLES"
  $ROOT_DIR/bin/cloudera-framework-hadoop.sh "fs -chown -R $USER_APP $ROOT_DIR_HDFS"
  for SCRIPT in $(find $ROOT_DIR_HDFS_INIT -maxdepth 1 -type f 2> /dev/null); do
    $SCRIPT
  done
fi

unset USER_ADMIN
unset DATABASE_DEFAULT

for SCRIPT in $(find $ROOT_DIR_HIVE_REFRESH -maxdepth 1 -type f 2> /dev/null); do
  if [ ${SCRIPT: -3} == ".sh" ]; then
    $SCRIPT
  else
    $ROOT_DIR/bin/cloudera-framework-hive.sh -f $SCRIPT
  fi
done
for SCRIPT in $(find $ROOT_DIR_IMPALA_REFRESH -maxdepth 1 -type f 2> /dev/null); do
  if [ ${SCRIPT: -3} == ".sh" ]; then
    $SCRIPT
  else
    $ROOT_DIR/bin/cloudera-framework-impala.sh -f $SCRIPT
  fi
done
