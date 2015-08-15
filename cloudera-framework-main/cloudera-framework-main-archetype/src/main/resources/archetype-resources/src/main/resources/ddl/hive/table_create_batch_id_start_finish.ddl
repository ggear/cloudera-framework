--
-- Table create
--

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:my.table.name}
PARTITIONED BY (
  ingest_batch_id STRING,
  ingest_batch_start BIGINT,
  ingest_batch_finish BIGINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT '${hivevar:my.table.format}'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '${hivevar:my.table.location}'
TBLPROPERTIES ('avro.schema.literal'='${hivevar:my.table.schema}');

MSCK REPAIR TABLE ${hivevar:my.table.name};
