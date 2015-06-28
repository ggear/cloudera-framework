--
-- Create and describe table
--

CREATE TABLE IF NOT EXISTS ${hivevar:test.table.name} (
  my_int INT,
  my_lowercase_string STRING,
  my_uppercase_string STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '${hivevar:test.table.field.delim}'
STORED AS TEXTFILE;

DESCRIBE ${hivevar:test.table.name};
