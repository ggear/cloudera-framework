--
-- Table create
--
-- noinspection SqlDialectInspectionForFile
-- noinspection SqlNoDataSourceInspectionForFile

USE mydataset;

CREATE EXTERNAL TABLE IF NOT EXISTS mytable (
  mydate DATE,
  myint INT,
  mydouble DOUBLE,
  myboolean BOOLEAN,
  mystring STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/mydataset/mytable';
