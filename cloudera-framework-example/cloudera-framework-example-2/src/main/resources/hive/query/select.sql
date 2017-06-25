--
-- Table select
--
-- noinspection SqlDialectInspectionForFile
-- noinspection SqlNoDataSourceInspectionForFile

USE mydataset;

SELECT
  *
FROM mytable;

SELECT
  mydate,
  myint
FROM mytable
WHERE
  mydate is not NULL and
  myint is not NULL;

SELECT
  from_unixtime(unix_timestamp(mydate), 'EEEE') as myday,
  sum(myint)
FROM mytable
WHERE
  mydate is not NULL and
  myint is not NULL
GROUP BY mydate;
