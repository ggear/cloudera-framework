--
-- Table create
--

USE ${hivevar:my.database.name};

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:my.table.name} (
  ingest_id STRING,
  ingest_batch STRING,
  ingest_timestamp BIGINT,
  my_timestamp BIGINT,
  my_integer INT,
  my_double DOUBLE,
  my_boolean BOOLEAN,
  my_string STRING
)
PARTITIONED BY ( ${hivevar:my.table.partition} )
STORED AS PARQUET
LOCATION '${hivevar:my.table.location}';

MSCK REPAIR TABLE ${hivevar:my.table.name};
