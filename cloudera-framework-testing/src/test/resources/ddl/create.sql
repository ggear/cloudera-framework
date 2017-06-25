--
-- Create and describe table
--
-- noinspection SqlDialectInspectionForFile
-- noinspection SqlNoDataSourceInspectionForFile

CREATE TABLE IF NOT EXISTS ${hivevar:test.table.name} (
	col1 INT,
	col2 INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '${hivevar:test.table.field.delim}'
STORED AS TEXTFILE;

DESCRIBE ${hivevar:test.table.name};
