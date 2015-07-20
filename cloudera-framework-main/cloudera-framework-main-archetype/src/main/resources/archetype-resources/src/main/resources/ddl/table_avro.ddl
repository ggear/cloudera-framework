--
-- Table create
--

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:my.table.name}
PARTITIONED BY (
  year SMALLINT,
  month TINYINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '${hivevar:my.table.location}'
TBLPROPERTIES ('avro.schema.literal'='${hivevar:my.table.avroschema}');

MSCK REPAIR TABLE ${hivevar:my.table.name};
