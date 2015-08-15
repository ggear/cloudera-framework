#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/*.env

set -x

CMD_LINE_ARGUMENTS="$1"
USER_ADMIN=${2:-"$USER_ADMIN"}
NAME_SPACE_SERVER=${3:-"$NAME_SPACE_SERVER"}
NAME_SPACE_DATABASE=${4:-"$NAME_SPACE_DATABASE"}
ROOT_DIR_HDFS=${5:-"$ROOT_DIR_HDFS"}
ROOT_DIR_HDFS_RAW_SOURCE=${6:-"$ROOT_DIR_HDFS_RAW_SOURCE"}
ROOT_DIR_HDFS_STAGED_PARTITIONED=${7:-"$ROOT_DIR_HDFS_STAGED_PARTITIONED"}
ROOT_DIR_HDFS_STAGED_MALFORMED=${8:-"$ROOT_DIR_HDFS_STAGED_MALFORMED"}
ROOT_DIR_HDFS_PROCESSED_CLEANSED=${9:-"$ROOT_DIR_HDFS_PROCESSED_CLEANSED"}
ROOT_DIR_HDFS_PROCESSED_DUPLICATE=${10:-"$ROOT_DIR_HDFS_PROCESSED_DUPLICATE"}
ROOT_DIR_HDFS_PROCESSED_MALFORMED=${11:-"$ROOT_DIR_HDFS_PROCESSED_MALFORMED"}

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

# TODO Enable all tables

TABLES_DDL=( \
#	"table_create_batch_name.ddl" \
#	"table_create_batch_name.ddl" \
#	"table_create_batch_id_start_finish.ddl" \
#	"table_create_batch_id_start_finish.ddl" \
#	"table_create_batch_name.ddl" \
#	"table_create_batch_name.ddl" \
	"table_create_batch_year_month.ddl" \
	"table_create_batch_year_month.ddl" \
#	"table_create_batch_name.ddl" \
#	"table_create_batch_name.ddl" \
)
TABLES_NAME=( \
#	"raw_source_text_xml_none" \
#	"raw_source_text_csv_none" \
#	"staged_partitioned_sequence_xml_none" \
#	"staged_partitioned_sequence_csv_none" \
#	"staged_malformed_text_xml_none" \
#	"staged_malformed_text_csv_none" \
	"processed_cleansed_avro_binary_none" \
	"processed_duplicate_avro_binary_none" \
#	"processed_malformed_xml_none" \
#	"processed_malformed_csv_none" \
)
TABLES_FORMAT=( \
#	"com.cloudera.example.model.input.RecordTextInputFormatXml" \
#	"com.cloudera.example.model.input.RecordTextInputFormatCsv" \
#	"com.cloudera.example.model.input.RecordSequenceInputFormatCsv" \
#	"com.cloudera.example.model.input.RecordSequenceInputFormatXml" \
#	"com.cloudera.example.model.input.RecordTextInputFormatXml" \
#	"com.cloudera.example.model.input.RecordTextInputFormatCsv" \
	"org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat" \
	"org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat" \
#	"com.cloudera.example.model.input.RecordTextInputFormatXml" \
#	"com.cloudera.example.model.input.RecordTextInputFormatCsv" \
)
TABLES_LOCATION=( \
#	"$ROOT_DIR_HDFS_RAW_SOURCE\text\xml\none" \
#	"$ROOT_DIR_HDFS_RAW_SOURCE\text\csv\none" \
#	"$ROOT_DIR_HDFS_STAGED_PARTITIONED\sequence\xml\none" \
#	"$ROOT_DIR_HDFS_STAGED_PARTITIONED\sequence\csv\none" \
#	"$ROOT_DIR_HDFS_STAGED_MALFORMED\text\xml\none" \
#	"$ROOT_DIR_HDFS_STAGED_MALFORMED\text\csv\none" \
	"$ROOT_DIR_HDFS_PROCESSED_CLEANSED\avro\binary\none" \
	"$ROOT_DIR_HDFS_PROCESSED_DUPLICATE\avro\binary\none" \
#	"$ROOT_DIR_HDFS_PROCESSED_MALFORMED\text\xml\none" \
#	"$ROOT_DIR_HDFS_PROCESSED_MALFORMED\text\csv\none" \
)

AVRO_SCHEMA="$(cat $ROOT_DIR/lib/cfg/avro/model.avsc | tr '\n' ' ' | tr '\t' ' ')"

for((i=0;i<${#TABLES_NAME[@]};i++)); do
  if ! $ROOT_DIR/bin/*-shell-hive.sh -e "MSCK REPAIR TABLE ${TABLES_NAME[$i]}; DESCRIBE ${TABLES_NAME[$i]}" 2> /dev/null; then
    $ROOT_DIR/bin/*-shell-hive.sh \
      --hivevar my.table.name="${TABLES_NAME[$i]}" \
      --hivevar my.table.format="${TABLES_FORMAT[$i]}" \
      --hivevar my.table.location="${TABLES_LOCATION[$i]}" \
      --hivevar my.table.schema="$AVRO_SCHEMA" \
      -f $ROOT_DIR/lib/ddl/hive/"${TABLES_DDL[$i]}"
    $ROOT_DIR/bin/*-shell-impala.sh -q "INVALIDATE METADATA ${TABLES_NAME[$i]};"
  else
    $ROOT_DIR/bin/*-shell-impala.sh -q "REFRESH ${TABLES_NAME[$i]};"
  fi
done
