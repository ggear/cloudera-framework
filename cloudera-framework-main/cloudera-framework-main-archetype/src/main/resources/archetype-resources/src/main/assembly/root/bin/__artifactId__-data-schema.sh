#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/*.env

set -x

CMD_LINE_ARGUMENTS="$1"
USER_ADMIN=${2:-"$USER_ADMIN"}
NAME_SPACE_SERVER=${3:-"$NAME_SPACE_SERVER"}
NAME_SPACE_DATABASE=${4:-"$NAME_SPACE_DATABASE"}
ROOT_DIR_HDFS=${5:-"$ROOT_DIR_HDFS"}
ROOT_DIR_HDFS_RAW_CANONICAL=${6:-"$ROOT_DIR_HDFS_RAW_CANONICAL"}
ROOT_DIR_HDFS_STAGED_CANONICAL=${7:-"$ROOT_DIR_HDFS_STAGED_CANONICAL"}
ROOT_DIR_HDFS_STAGED_MALFORMED=${8:-"$ROOT_DIR_HDFS_STAGED_MALFORMED"}
ROOT_DIR_HDFS_PARTITIONED_CANONICAL=${9:-"$ROOT_DIR_HDFS_PARTITIONED_CANONICAL"}
ROOT_DIR_HDFS_PARTITIONED_DUPLICATE=${10:-"$ROOT_DIR_HDFS_PARTITIONED_DUPLICATE"}
ROOT_DIR_HDFS_PARTITIONED_MALFORMED=${11:-"$ROOT_DIR_HDFS_PARTITIONED_MALFORMED"}
ROOT_DIR_HDFS_PROCESSED_CANONICAL=${12:-"$ROOT_DIR_HDFS_PROCESSED_CANONICAL"}
ROOT_DIR_HDFS_PROCESSED_DUPLICATE=${13:-"$ROOT_DIR_HDFS_PROCESSED_DUPLICATE"}
ROOT_DIR_HDFS_PROCESSED_MALFORMED=${14:-"$ROOT_DIR_HDFS_PROCESSED_MALFORMED"}

if [ $($ROOT_DIR/bin/*-shell-impala.sh -q "SHOW ROLES" 2> /dev/null| grep $USER_ADMIN|wc -l) -eq 0 ]; then
  $ROOT_DIR/bin/*-shell-hive.sh \
    --hivevar my.user=$USER_ADMIN \
    --hivevar my.server.name=$NAME_SPACE_SERVER \
    --hivevar my.database.name=$NAME_SPACE_DATABASE \
    --hivevar my.database.location=$ROOT_DIR_HDFS \
    -f $ROOT_DIR/lib/ddl/hive/database.ddl
  until $ROOT_DIR/bin/*-shell-impala.sh -r -q "SHOW TABLES"; do
  	echo "Sleeping while waiting for database and roles to sync ... "
    sleep 5
  done
fi

TABLES_DDL=( \
	"table_create.ddl" \
	"table_create.ddl" \
)
TABLES_NAME=( \
	"mydataset_partitioned_canonical_avro_binary_none" \
	"mydataset_partitioned_duplicate_avro_binary_none" \
)
TABLES_PARTITION=( \
	"my_timestamp_year TINYINT, my_timestamp_month TINYINT" \
	"my_timestamp_year TINYINT, my_timestamp_month TINYINT" \
)
TABLES_SERDE=( \
	"org.apache.hadoop.hive.serde2.avro.AvroSerDe" \
	"org.apache.hadoop.hive.serde2.avro.AvroSerDe" \
)
TABLES_INPUT=( \
	"org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat" \
	"org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat" \
)
TABLES_OUTPUT=( \
	"org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat" \
	"org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat" \
)
TABLES_LOCATION=( \
	"$ROOT_DIR_HDFS_PROCESSED_CANONICAL\avro\binary\none" \
	"$ROOT_DIR_HDFS_PROCESSED_DUPLICATE\avro\binary\none" \
)

AVRO_SCHEMA="$(cat $ROOT_DIR/lib/cfg/avro/model.avsc | tr '\n' ' ' | tr '\t' ' ')"

for((i=0;i<${#TABLES_NAME[@]};i++)); do
  if ! $ROOT_DIR/bin/*-shell-hive.sh -e "MSCK REPAIR TABLE ${TABLES_NAME[$i]}; DESCRIBE ${TABLES_NAME[$i]}" 2> /dev/null; then
    $ROOT_DIR/bin/*-shell-hive.sh \
      --hivevar my.table.name="${TABLES_NAME[$i]}" \
      --hivevar my.table.partition="${TABLES_PARTITION[$i]}" \
      --hivevar my.table.serde="${TABLES_SERDE[$i]}" \
      --hivevar my.table.input="${TABLES_INPUT[$i]}" \
      --hivevar my.table.output="${TABLES_OUTPUT[$i]}" \
      --hivevar my.table.location="${TABLES_LOCATION[$i]}" \
      --hivevar my.table.schema="$AVRO_SCHEMA" \
      -f $ROOT_DIR/lib/ddl/hive/"${TABLES_DDL[$i]}"
    $ROOT_DIR/bin/*-shell-impala.sh -q "INVALIDATE METADATA ${TABLES_NAME[$i]};"
  else
    $ROOT_DIR/bin/*-shell-impala.sh -q "REFRESH ${TABLES_NAME[$i]};"
  fi
done
