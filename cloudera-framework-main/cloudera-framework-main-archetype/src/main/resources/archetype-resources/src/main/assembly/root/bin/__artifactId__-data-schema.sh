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
ROOT_DIR_HDFS_PROCESSED_REWRITTEN=${13:-"$ROOT_DIR_HDFS_PROCESSED_REWRITTEN"}
ROOT_DIR_HDFS_PROCESSED_DUPLICATE=${14:-"$ROOT_DIR_HDFS_PROCESSED_DUPLICATE"}
ROOT_DIR_HDFS_PROCESSED_MALFORMED=${15:-"$ROOT_DIR_HDFS_PROCESSED_MALFORMED"}

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
	"table_create_avro.ddl" \
	"table_create_avro.ddl" \
	"table_create_avro.ddl" \
	"table_create_avro.ddl" \
	"table_create_text.ddl" \
	"table_create_text.ddl" \
	"table_create_avro.ddl" \
	"table_create_avro.ddl" \
	"table_create_text.ddl" \
	"table_create_text.ddl" \
	"table_create_parquet.ddl" \
	"table_create_parquet.ddl" \
	"table_create_parquet.ddl" \
)
TABLES_NAME=( \
	"mydataset_raw_canonical_text_csv_none" \
	"mydataset_raw_canonical_text_xml_none" \
	"mydataset_staged_canonical_sequence_csv_none" \
	"mydataset_staged_canonical_sequence_xml_none" \
	"mydataset_staged_malformed_text_csv_none" \
	"mydataset_staged_malformed_text_xml_none" \
	"mydataset_partitioned_canonical_avro_binary_none" \
	"mydataset_partitioned_duplicate_avro_binary_none" \
	"mydataset_partitioned_malformed_text_csv_none" \
	"mydataset_partitioned_malformed_text_xml_none" \
	"mydataset_processed_canonical_parquet_dict_snappy" \
	"mydataset_processed_rewritten_parquet_dict_snappy" \
	"mydataset_processed_duplicate_parquet_dict_snappy" \
)
TABLES_PARTITION=( \
	"ingest_batch_name STRING" \
	"ingest_batch_name STRING" \
	"ingest_batch_id STRING, ingest_batch_start STRING, ingest_batch_finish STRING" \
	"ingest_batch_id STRING, ingest_batch_start STRING, ingest_batch_finish STRING" \
	"ingest_batch_name STRING" \
	"ingest_batch_name STRING" \
	"my_timestamp_year TINYINT, my_timestamp_month TINYINT" \
	"my_timestamp_year TINYINT, my_timestamp_month TINYINT" \
	"ingest_batch_name STRING" \
	"ingest_batch_name STRING" \
	"my_timestamp_year TINYINT, my_timestamp_month TINYINT" \
	"my_timestamp_year TINYINT, my_timestamp_month TINYINT" \
	"my_timestamp_year TINYINT, my_timestamp_month TINYINT" \
)
TABLES_SERDE=( \
	"org.apache.hadoop.hive.serde2.avro.AvroSerDe" \
	"org.apache.hadoop.hive.serde2.avro.AvroSerDe" \
	"org.apache.hadoop.hive.serde2.avro.AvroSerDe" \
	"org.apache.hadoop.hive.serde2.avro.AvroSerDe" \
	"org.apache.hadoop.hive.serde2.RegexSerDe" \
	"org.apache.hadoop.hive.serde2.RegexSerDe" \
	"org.apache.hadoop.hive.serde2.avro.AvroSerDe" \
	"org.apache.hadoop.hive.serde2.avro.AvroSerDe" \
	"org.apache.hadoop.hive.serde2.RegexSerDe" \
	"org.apache.hadoop.hive.serde2.RegexSerDe" \
	"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe" \
	"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe" \
	"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe" \
)
TABLES_INPUT=( \
	"com.cloudera.example.model.input.hive.RecordTextInputFormatCsv" \
	"com.cloudera.example.model.input.hive.RecordTextInputFormatXml" \
	"com.cloudera.example.model.input.hive.RecordSequenceInputFormatCsv" \
	"com.cloudera.example.model.input.hive.RecordSequenceInputFormatXml" \
	"org.apache.hadoop.mapred.TextInputFormat" \
	"org.apache.hadoop.mapred.TextInputFormat" \
	"org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat" \
	"org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat" \
	"org.apache.hadoop.mapred.TextInputFormat" \
	"org.apache.hadoop.mapred.TextInputFormat" \
	"org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat" \
	"org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat" \
	"org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat" \
)
TABLES_OUTPUT=( \
	"org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat" \
	"org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat" \
	"org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat" \
	"org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat" \
	"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat" \
	"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat" \
	"org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat" \
	"org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat" \
	"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat" \
	"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat" \
	"org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat" \
	"org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat" \
	"org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat" \
)
TABLES_LOCATION=( \
	"$ROOT_DIR_HDFS_RAW_CANONICAL/text/csv/none" \
	"$ROOT_DIR_HDFS_RAW_CANONICAL/text/xml/none" \
	"$ROOT_DIR_HDFS_STAGED_CANONICAL/sequence/csv/none" \
	"$ROOT_DIR_HDFS_STAGED_CANONICAL/sequence/xml/none" \
	"$ROOT_DIR_HDFS_STAGED_MALFORMED/text/csv/none" \
	"$ROOT_DIR_HDFS_STAGED_MALFORMED/text/xml/none" \
	"$ROOT_DIR_HDFS_PARTITIONED_CANONICAL/avro/binary/none" \
	"$ROOT_DIR_HDFS_PARTITIONED_DUPLICATE/avro/binary/none" \
	"$ROOT_DIR_HDFS_PARTITIONED_MALFORMED/text/csv/none" \
	"$ROOT_DIR_HDFS_PARTITIONED_MALFORMED/text/xml/none" \
	"$ROOT_DIR_HDFS_PROCESSED_CANONICAL/parquet/dict/snappy" \
	"$ROOT_DIR_HDFS_PROCESSED_REWRITTEN/parquet/dict/snappy" \
	"$ROOT_DIR_HDFS_PROCESSED_DUPLICATE/parquet/dict/snappy" \
)

AVRO_SCHEMA="$(cat $ROOT_DIR/lib/cfg/avro/model.avsc | tr '\n' ' ' | tr '\t' ' ')"

for((i=0;i<${#TABLES_NAME[@]};i++)); do
  if ! $ROOT_DIR/bin/*-shell-hive.sh -e "MSCK REPAIR TABLE ${TABLES_NAME[$i]}; DESCRIBE ${TABLES_NAME[$i]}" 2> /dev/null; then
    $ROOT_DIR/bin/*-shell-hive.sh \
      --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat
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
