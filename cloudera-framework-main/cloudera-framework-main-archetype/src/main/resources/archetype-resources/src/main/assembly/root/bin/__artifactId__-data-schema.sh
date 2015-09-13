#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/*.env

set -x

CMD_LINE_ARGUMENTS="$1"
DROP_SCHEMA=${3:-false}
USER_ADMIN=${3:-"$USER_ADMIN"}
NAME_SPACE_SERVER=${4:-"$NAME_SPACE_SERVER"}
NAME_SPACE_DATABASE=${5:-"$NAME_SPACE_DATABASE"}
CLUSTER_NODE_USER=${6:-"$CLUSTER_NODE_USER"}
CLUSTER_NODE_KEY=${7:-"$CLUSTER_NODE_KEY"}
MANAGER_SERVER_USER=${8:-"admin"}
MANAGER_SERVER_PWORD=${9:-"admin"}
MANAGER_SERVER_HOST=${10:-"$MANAGER_SERVER_HOST"}
MANAGER_SERVER_PORT=${11:-"$MANAGER_SERVER_PORT"}
HIVE_HIVESERVER2_HOSTS=${12:-"$HIVE_HIVESERVER2_HOSTS"}
HIVESERVER2_DIR_LIB=${13:-"/opt/local/hive/lib/"}
ROOT_DIR_HDFS=${14:-"$ROOT_DIR_HDFS"}
ROOT_DIR_HDFS_RAW_CANONICAL=${15:-"$ROOT_DIR_HDFS_RAW_CANONICAL"}
ROOT_DIR_HDFS_STAGED_CANONICAL=${16:-"$ROOT_DIR_HDFS_STAGED_CANONICAL"}
ROOT_DIR_HDFS_STAGED_MALFORMED=${17:-"$ROOT_DIR_HDFS_STAGED_MALFORMED"}
ROOT_DIR_HDFS_PARTITIONED_CANONICAL=${18:-"$ROOT_DIR_HDFS_PARTITIONED_CANONICAL"}
ROOT_DIR_HDFS_PARTITIONED_DUPLICATE=${19:-"$ROOT_DIR_HDFS_PARTITIONED_DUPLICATE"}
ROOT_DIR_HDFS_PARTITIONED_MALFORMED=${20:-"$ROOT_DIR_HDFS_PARTITIONED_MALFORMED"}
ROOT_DIR_HDFS_PROCESSED_CANONICAL=${21:-"$ROOT_DIR_HDFS_PROCESSED_CANONICAL"}
ROOT_DIR_HDFS_PROCESSED_REWRITTEN=${22:-"$ROOT_DIR_HDFS_PROCESSED_REWRITTEN"}
ROOT_DIR_HDFS_PROCESSED_DUPLICATE=${23:-"$ROOT_DIR_HDFS_PROCESSED_DUPLICATE"}

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

if [ $DROP_SCHEMA ]; then

  if [ $($ROOT_DIR/bin/*-shell-impala.sh -q "SHOW ROLES" 2> /dev/null| grep $USER_ADMIN|wc -l) -eq 0 ]; then
    $ROOT_DIR/bin/*-shell-hive.sh --outputformat=vertical -e "SHOW TABLES" | grep tab_name | awk '{print $2}' | xargs -I £ $ROOT_DIR/bin/*-shell-hive.sh -e "DROP TABLE £"
    $ROOT_DIR/bin/*-shell-impala.sh -r -q "USE default; DROP DATABASE $NAME_SPACE_DATABASE; DROP ROLE $USER_ADMIN;"
  fi

fi

if [ $($ROOT_DIR/bin/*-shell-impala.sh -q "SHOW ROLES" 2> /dev/null| grep $USER_ADMIN|wc -l) -eq 0 ]; then
  
  HIVE_HIVESERVER2_HOSTS_ARRAY=(${HIVE_HIVESERVER2_HOSTS//,/ })
  for HIVE_HIVESERVER2_HOST in "${HIVE_HIVESERVER2_HOSTS_ARRAY[@]}"; do
    ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i $CLUSTER_NODE_KEY $CLUSTER_NODE_USER@$HIVE_HIVESERVER2_HOST \
  	  "mkdir -p $HIVESERVER2_DIR_LIB"
    scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i $CLUSTER_NODE_KEY \
      $ROOT_DIR/lib/jar/*.jar $ROOT_DIR/lib/jar/dep/*.jar $CLUSTER_NODE_USER@$HIVE_HIVESERVER2_HOST:$HIVESERVER2_DIR_LIB
  done  
  
  python - "$MANAGER_SERVER_USER" "$MANAGER_SERVER_PWORD" "$MANAGER_SERVER_HOST" "$MANAGER_SERVER_PORT" "$HIVESERVER2_DIR_LIB" << END
import sys
from cm_api import api_client
from cm_api.api_client import ApiResource
def main(argv):
  print ''
  api = ApiResource(argv[3], argv[4], argv[1], argv[2], False, 10);
  for cluster in api.get_all_clusters():
    for service in cluster.get_all_services():
      if service.type == 'HIVE':
        print 'Updating Hive config ...'
        service.update_config({'hive_aux_jars_path_dir': argv[5]})
        print 'Restarting Hive service ...'
        service.restart().wait()
  return 0
if __name__ == '__main__':
  sys.exit(main(sys.argv))
END
  
  $ROOT_DIR/bin/*-shell-hive.sh \
    --hivevar my.user=$USER_ADMIN \
    --hivevar my.server.name=$NAME_SPACE_SERVER \
    --hivevar my.database.name=$NAME_SPACE_DATABASE \
    --hivevar my.database.location=$ROOT_DIR_HDFS \
    -f $ROOT_DIR/lib/ddl/hive/database_create.ddl
  until $ROOT_DIR/bin/*-shell-impala.sh -r -q "SHOW TABLES"; do
  	echo "Sleeping while waiting for database and roles to sync ... "
    sleep 5
  done

  AVRO_SCHEMA="$(cat $ROOT_DIR/lib/cfg/avro/model.avsc | tr '\n' ' ' | tr '\t' ' ')"

  for((i=0;i<${#TABLES_NAME[@]};i++)); do
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
  done  
  
fi

TABLES_REFRESH_HIVE=""  
TABLES_REFRESH_IMPALA=""  
for((i=0;i<${#TABLES_NAME[@]};i++)); do
  TABLES_REFRESH_HIVE="$TABLES_REFRESH_HIVE"" MSCK REPAIR TABLE ""${TABLES_NAME[$i]}""; "
  TABLES_REFRESH_IMPALA="$TABLES_REFRESH_IMPALA"" REFRESH ""${TABLES_NAME[$i]}""; "
done
$ROOT_DIR/bin/*-shell-hive.sh -q "$TABLES_REFRESH_HIVE"
$ROOT_DIR/bin/*-shell-impala.sh -q "$TABLES_REFRESH_IMPALA"
