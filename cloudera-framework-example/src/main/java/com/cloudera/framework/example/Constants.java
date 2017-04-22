package com.cloudera.framework.example;

/**
 * Constants
 */
public interface Constants {

  String DIR_REL_MYDS = "mydataset";
  String DIR_ABS_MYDS = "/mydataset";

  String DIR_REL_MYDS_RAW = "raw";
  String DIR_REL_MYDS_STAGED = "staged";
  String DIR_REL_MYDS_PARTITIONED = "partitioned";
  String DIR_REL_MYDS_CLEANSED = "cleansed";

  String DIR_REL_MYDS_CANONICAL = "canonical";
  String DIR_REL_MYDS_REWRITTEN = "rewritten";
  String DIR_REL_MYDS_DUPLICATE = "duplicate";
  String DIR_REL_MYDS_MALFORMED = "malformed";

  String DIR_ABS_MYDS_RAW = "/mydataset/raw";
  String DIR_ABS_MYDS_RAW_CANONICAL = "/mydataset/raw/canonical";
  String DIR_REL_MYDS_RAW_CANONICAL_CSV = "canonical/text/csv/none";
  String DIR_ABS_MYDS_RAW_CANONICAL_CSV = "/mydataset/raw/canonical/text/csv/none";
  String DIR_REL_MYDS_RAW_CANONICAL_XML = "canonical/text/xml/none";
  String DIR_ABS_MYDS_RAW_CANONICAL_XML = "/mydataset/raw/canonical/text/xml/none";

  String DIR_ABS_MYDS_STAGED = "/mydataset/staged";
  String DIR_ABS_MYDS_STAGED_CANONICAL = "/mydataset/staged/canonical";
  String DIR_REL_MYDS_STAGED_CANONICAL_CSV = "canonical/sequence/csv/none";
  String DIR_ABS_MYDS_STAGED_CANONICAL_CSV = "/mydataset/staged/canonical/sequence/csv/none";
  String DIR_REL_MYDS_STAGED_CANONICAL_XML = "canonical/sequence/xml/none";
  String DIR_ABS_MYDS_STAGED_CANONICAL_XML = "/mydataset/staged/canonical/sequence/xml/none";
  String DIR_ABS_MYDS_STAGED_MALFORMED = "/mydataset/staged/malformed";
  String DIR_REL_MYDS_STAGED_MALFORMED_CSV = "malformed/text/csv/none";
  String DIR_ABS_MYDS_STAGED_MALFORMED_CSV = "/mydataset/staged/malformed/text/csv/none";
  String DIR_REL_MYDS_STAGED_MALFORMED_XML = "malformed/text/xml/none";
  String DIR_ABS_MYDS_STAGED_MALFORMED_XML = "/mydataset/staged/malformed/text/xml/none";

  String DIR_ABS_MYDS_PARTITIONED = "/mydataset/partitioned";
  String DIR_ABS_MYDS_PARTITIONED_CANONICAL = "/mydataset/partitioned/canonical";
  String DIR_REL_MYDS_PARTITIONED_CANONICAL_AVRO = "canonical/avro/binary/none";
  String DIR_ABS_MYDS_PARTITIONED_CANONICAL_AVRO = "/mydataset/partitioned/canonical/avro/binary/none";
  String DIR_ABS_MYDS_PARTITIONED_DUPLICATE = "/mydataset/partitioned/duplicate";
  String DIR_REL_MYDS_PARTITIONED_DUPLICATE_AVRO = "duplicate/avro/binary/none";
  String DIR_ABS_MYDS_PARTITIONED_DUPLICATE_AVRO = "/mydataset/partitioned/duplicate/avro/binary/none";
  String DIR_ABS_MYDS_PARTITIONED_MALFORMED = "/mydataset/partitioned/malformed";
  String DIR_REL_MYDS_PARTITIONED_MALFORMED_CSV = "malformed/text/csv/none";
  String DIR_ABS_MYDS_PARTITIONED_MALFORMED_CSV = "/mydataset/partitioned/malformed/text/csv/none";
  String DIR_REL_MYDS_PARTITIONED_MALFORMED_XML = "malformed/text/xml/none";
  String DIR_ABS_MYDS_PARTITIONED_MALFORMED_XML = "/mydataset/partitioned/malformed/text/xml/none";

  String DIR_ABS_MYDS_CLEANSED = "/mydataset/cleansed";
  String DIR_ABS_MYDS_CLEANSED_CANONICAL = "/mydataset/cleansed/canonical";
  String DIR_REL_MYDS_CLEANSED_CANONICAL_PARQUET = "canonical/parquet/dict/snappy";
  String DIR_ABS_MYDS_CLEANSED_CANONICAL_PARQUET = "/mydataset/cleansed/canonical/parquet/dict/snappy";
  String DIR_ABS_MYDS_CLEANSED_REWRITTEN = "/mydataset/cleansed/rewritten";
  String DIR_REL_MYDS_CLEANSED_REWRITTEN_PARQUET = "rewritten/parquet/dict/snappy";
  String DIR_ABS_MYDS_CLEANSED_REWRITTEN_PARQUET = "/mydataset/cleansed/rewritten/parquet/dict/snappy";
  String DIR_ABS_MYDS_CLEANSED_DUPLICATE = "/mydataset/cleansed/duplicate";
  String DIR_REL_MYDS_CLEANSED_DUPLICATE_PARQUET = "duplicate/parquet/dict/snappy";
  String DIR_ABS_MYDS_CLEANSED_DUPLICATE_PARQUET = "/mydataset/cleansed/duplicate/parquet/dict/snappy";

  String DDL_FILE_BATCH_NAME = "batch_name.ddl";
  String DDL_FILE_BATCH_ID_START_FINISH = "batch_id_start_finish.ddl";

  String DDL_FILE_CREATE_TEXT = "text.ddl";
  String DDL_FILE_CREATE_AVRO = "avro.ddl";
  String DDL_FILE_CREATE_PARQUET = "parquet.ddl";

  String DDL_VAR_FILE = "my.table.file";
  String DDL_VAR_ROWS = "my.table.rows";
  String DDL_VAR_NAME = "my.table.name";
  String DDL_VAR_SERDE = "my.table.serde";
  String DDL_VAR_INPUT = "my.table.input";
  String DDL_VAR_OUTPUT = "my.table.output";
  String DDL_VAR_SCHEMA = "my.table.schema";
  String DDL_VAR_ROOT = "my.table.root";
  String DDL_VAR_LOCATION = "my.table.location";
  String DDL_VAR_PARTITION = "my.table.partition";
  String DDL_VAR_DATABASE = "my.database.name";

  String MODEL_AVRO_FILE = "/avro/model.avsc";

  String CONFIG_INPUT_PATH = "cloudera.input.dir";
  String CONFIG_OUTPUT_PATH = "cloudera.output.dir";

}
