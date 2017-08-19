--
-- Table create
--
-- noinspection SqlDialectInspectionForFile
-- noinspection SqlNoDataSourceInspectionForFile

USE ${hivevar:my.database.name};

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:my.table.name} (
  my_raw_data STRING
)
PARTITIONED BY ( ${hivevar:my.table.partition} )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
  "input.regex" = "(.*)"
)
STORED AS INPUTFORMAT '${hivevar:my.table.input}'
OUTPUTFORMAT '${hivevar:my.table.output}'
LOCATION '${hivevar:my.table.location}';

MSCK REPAIR TABLE ${hivevar:my.table.name};
