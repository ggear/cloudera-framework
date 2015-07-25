--
-- Table create
--

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:cyclehire.table.name} (
  my_timestamp LONG,
  my_integer INT,
  my_double DOUBLE,
  my_boolean BOOLEAN,
  my_string STRING
)
PARTITIONED BY (
  year SMALLINT,
  month TINYINT
)
STORED AS PARQUET
LOCATION '${hivevar:cyclehire.table.location}';

MSCK REPAIR TABLE ${hivevar:my.table.name};
