--
-- Table create
--

USE ${hivevar:my.database.name};

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:my.table.name}
PARTITIONED BY ( ${hivevar:my.table.partition} )
ROW FORMAT SERDE '${hivevar:my.table.serde}'
STORED AS INPUTFORMAT '${hivevar:my.table.input}'
OUTPUTFORMAT '${hivevar:my.table.output}'
LOCATION '${hivevar:my.table.location}'
TBLPROPERTIES ('avro.schema.literal'='${hivevar:my.table.schema}');

MSCK REPAIR TABLE ${hivevar:my.table.name};
