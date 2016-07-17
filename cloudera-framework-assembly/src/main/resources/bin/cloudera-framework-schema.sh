#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/*.env

set -x

CMD_LINE_ARGUMENTS="$1"
DROP_SCHEMA=${2:-false}
USER_APP=${3:-"$USER_APP"}
USER_SERVER=${4:-"$USER_SERVER"}
DATABASE_APP=${5:-"$DATABASE_APP"}
ROOT_DIR_HIVE_SCHEMA=${6:-"$ROOT_DIR/lib/hive/schema"}
ROOT_DIR_HIVE_REFRESH=${7:-"$ROOT_DIR/lib/hive/refresh"}
ROOT_DIR_IMPALA_SCHEMA=${8:-"$ROOT_DIR/lib/impala/schema"}
ROOT_DIR_IMPALA_REFRESH=${9:-"$ROOT_DIR/lib/impala/refresh"}
ROOT_DIR_HDFS_INIT=${9:-"$ROOT_DIR/lib/hdfs"}
ROOT_DIR_HDFS=${10:-"$ROOT_DIR_HDFS"}

export USER_ADMIN=true

if $DROP_SCHEMA; then
  if [ $($ROOT_DIR/bin/cloudera-framework-impala.sh -d default -q "SHOW DATABASES" 2> /dev/null| grep $DATABASE_APP|wc -l) -gt 0 ]; then
    $ROOT_DIR/bin/cloudera-framework-impala.sh -d default -q "DROP DATABASE IF EXISTS $DATABASE_APP CASCADE"
  fi
  if [ $($ROOT_DIR/bin/cloudera-framework-impala.sh -d default -q "SHOW ROLES" 2> /dev/null| grep $USER_APP|wc -l) -gt 0 ]; then
    $ROOT_DIR/bin/cloudera-framework-impala.sh -d default -q "DROP ROLE $USER_APP;"
  fi
  exit 0
fi

if [ $($ROOT_DIR/bin/cloudera-framework-impala.sh -d default -q "SHOW ROLES" 2> /dev/null| grep $USER_APP|wc -l) -eq 0 ]; then
  $ROOT_DIR/bin/cloudera-framework-hadoop.sh "fs -mkdir -p $ROOT_DIR_HDFS"
  $ROOT_DIR/bin/cloudera-framework-hadoop.sh "fs -chmod 777 $ROOT_DIR_HDFS"
  for SCRIPT in $(ls $ROOT_DIR_HDFS_INIT 2> /dev/null); do
    $ROOT_DIR_HDFS_INIT/$SCRIPT
  done
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
  until $ROOT_DIR/bin/cloudera-framework-impala.sh -r -q "SHOW TABLES"; do
  	echo "Sleeping while waiting for database and roles to sync ... "
    sleep 5
  done
  for SCRIPT in $(ls $ROOT_DIR_HIVE_SCHEMA 2> /dev/null); do
    if [ ${SCRIPT: -3} == ".sh" ]; then
      $ROOT_DIR_HIVE_SCHEMA/$SCRIPT
    else
      $ROOT_DIR/bin/cloudera-framework-hive.sh -f $ROOT_DIR_HIVE_SCHEMA/$SCRIPT
    fi
  done
  for SCRIPT in $(ls $ROOT_DIR_IMPALA_SCHEMA 2> /dev/null); do
    if [ ${SCRIPT: -3} == ".sh" ]; then
      $ROOT_DIR_IMPALA_SCHEMA/$SCRIPT
    else
      $ROOT_DIR/bin/cloudera-framework-impala.sh -f $ROOT_DIR_IMPALA_SCHEMA/$SCRIPT
    fi
  done
  $ROOT_DIR/bin/cloudera-framework-impala.sh -r -q "SHOW TABLES"
fi

for SCRIPT in $(ls $ROOT_DIR_HIVE_REFRESH 2> /dev/null); do
  if [ ${SCRIPT: -3} == ".sh" ]; then
    $ROOT_DIR_HIVE_REFRESH/$SCRIPT
  else
    $ROOT_DIR/bin/cloudera-framework-hive.sh -f $ROOT_DIR_HIVE_REFRESH/$SCRIPT
  fi
done
for SCRIPT in $(ls $ROOT_DIR_IMPALA_REFRESH 2> /dev/null); do
  if [ ${SCRIPT: -3} == ".sh" ]; then
    $ROOT_DIR_IMPALA_REFRESH/$SCRIPT
  else
    $ROOT_DIR/bin/cloudera-framework-impala.sh -f $ROOT_DIR_IMPALA_REFRESH/$SCRIPT
  fi
done
