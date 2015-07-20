--
-- Run queries accross dataset
--

DESCRIBE ${hivevar:my.table.name};

SELECT 
  COUNT(1) AS number_of_records
FROM ${hivevar:my.table.name};

