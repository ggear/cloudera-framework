#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/../../..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

CREATE_SCHEMA=${1:-true}

TABLES_DDL=( \
	"avro.ddl" \
	"avro.ddl" \
	"avro.ddl" \
	"avro.ddl" \
	"text.ddl" \
	"text.ddl" \
	"avro.ddl" \
	"avro.ddl" \
	"text.ddl" \
	"text.ddl" \
	"parquet.ddl" \
	"parquet.ddl" \
	"parquet.ddl" \
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
	"mydataset_cleansed_canonical_parquet_dict_snappy" \
	"mydataset_cleansed_rewritten_parquet_dict_snappy" \
	"mydataset_cleansed_duplicate_parquet_dict_snappy" \
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
	"com.cloudera.framework.example.one.model.input.hive.RecordTextInputFormatCsv" \
	"com.cloudera.framework.example.one.model.input.hive.RecordTextInputFormatXml" \
	"com.cloudera.framework.example.one.model.input.hive.RecordSequenceInputFormatCsv" \
	"com.cloudera.framework.example.one.model.input.hive.RecordSequenceInputFormatXml" \
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
	"$ROOT_DIR_HDFS_CLEANSED_CANONICAL/parquet/dict/snappy" \
	"$ROOT_DIR_HDFS_CLEANSED_REWRITTEN/parquet/dict/snappy" \
	"$ROOT_DIR_HDFS_CLEANSED_DUPLICATE/parquet/dict/snappy" \
)

if $CREATE_SCHEMA; then
  AVRO_SCHEMA="$(cat $ROOT_DIR/lib/avro/model.avsc | tr '\n' ' ' | tr '\t' ' ')"
  for((i=0;i<${#TABLES_NAME[@]};i++)); do
    $ROOT_DIR/bin/cloudera-framework-hive.sh \
      --hivevar my.database.name="$DATABASE_APP" \
      --hivevar my.table.name="${TABLES_NAME[$i]}" \
      --hivevar my.table.partition="${TABLES_PARTITION[$i]}" \
      --hivevar my.table.serde="${TABLES_SERDE[$i]}" \
      --hivevar my.table.input="${TABLES_INPUT[$i]}" \
      --hivevar my.table.output="${TABLES_OUTPUT[$i]}" \
      --hivevar my.table.location="${TABLES_LOCATION[$i]}" \
      --hivevar my.table.schema="$AVRO_SCHEMA" \
      -f $ROOT_DIR/lib/hive/schema/ddl/"${TABLES_DDL[$i]}"
    $ROOT_DIR/bin/cloudera-framework-impala.sh -q "USE $DATABASE_APP; INVALIDATE METADATA ${TABLES_NAME[$i]};"
  done
fi

if ! $CREATE_SCHEMA; then
  TABLES_REFRESH_HIVE=""
  for((i=0;i<${#TABLES_NAME[@]};i++)); do
    TABLES_REFRESH_HIVE="$TABLES_REFRESH_HIVE"" MSCK REPAIR TABLE ""${TABLES_NAME[$i]}""; "
  done
  $ROOT_DIR/bin/cloudera-framework-hive.sh -e "$TABLES_REFRESH_HIVE"
  set +e
  for((i=0;i<${#TABLES_NAME[@]};i++)); do
    $ROOT_DIR/bin/cloudera-framework-impala.sh -q "REFRESH ${TABLES_NAME[$i]};" 2> /dev/null
  done
  set -e
fi
