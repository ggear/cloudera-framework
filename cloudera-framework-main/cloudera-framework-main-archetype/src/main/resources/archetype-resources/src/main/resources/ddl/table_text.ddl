--
-- Table create
--

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:my.table.name} (
  my_timestamp BIGINT,
  my_integer INT,
  my_double DOUBLE,
  my_boolean BOOLEAN,
  my_string STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '${hivevar:my.table.delim}'
STORED AS TEXTFILE
LOCATION '${hivevar:my.table.location}';

MSCK REPAIR TABLE ${hivevar:my.table.name};