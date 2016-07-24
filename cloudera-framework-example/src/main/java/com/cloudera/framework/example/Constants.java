package com.cloudera.framework.example;

/**
 * Constants
 */
public interface Constants {

  public static final String DIR_REL_MYDS = "mydataset";
  public static final String DIR_ABS_MYDS = "/mydataset";

  public static final String DIR_REL_MYDS_RAW = "raw";
  public static final String DIR_REL_MYDS_STAGED = "staged";
  public static final String DIR_REL_MYDS_PARTITIONED = "partitioned";
  public static final String DIR_REL_MYDS_CLEANSED = "cleansed";

  public static final String DIR_REL_MYDS_CANONICAL = "canonical";
  public static final String DIR_REL_MYDS_REWRITTEN = "rewritten";
  public static final String DIR_REL_MYDS_DUPLICATE = "duplicate";
  public static final String DIR_REL_MYDS_MALFORMED = "malformed";

  public static final String DIR_ABS_MYDS_RAW = "/mydataset/raw";
  public static final String DIR_ABS_MYDS_RAW_CANONICAL = "/mydataset/raw/canonical";
  public static final String DIR_REL_MYDS_RAW_CANONICAL_CSV = "canonical/text/csv/none";
  public static final String DIR_ABS_MYDS_RAW_CANONICAL_CSV = "/mydataset/raw/canonical/text/csv/none";
  public static final String DIR_REL_MYDS_RAW_CANONICAL_XML = "canonical/text/xml/none";
  public static final String DIR_ABS_MYDS_RAW_CANONICAL_XML = "/mydataset/raw/canonical/text/xml/none";

  public static final String DIR_ABS_MYDS_STAGED = "/mydataset/staged";
  public static final String DIR_ABS_MYDS_STAGED_CANONICAL = "/mydataset/staged/canonical";
  public static final String DIR_REL_MYDS_STAGED_CANONICAL_CSV = "canonical/sequence/csv/none";
  public static final String DIR_ABS_MYDS_STAGED_CANONICAL_CSV = "/mydataset/staged/canonical/sequence/csv/none";
  public static final String DIR_REL_MYDS_STAGED_CANONICAL_XML = "canonical/sequence/xml/none";
  public static final String DIR_ABS_MYDS_STAGED_CANONICAL_XML = "/mydataset/staged/canonical/sequence/xml/none";
  public static final String DIR_ABS_MYDS_STAGED_MALFORMED = "/mydataset/staged/malformed";
  public static final String DIR_REL_MYDS_STAGED_MALFORMED_CSV = "malformed/text/csv/none";
  public static final String DIR_ABS_MYDS_STAGED_MALFORMED_CSV = "/mydataset/staged/malformed/text/csv/none";
  public static final String DIR_REL_MYDS_STAGED_MALFORMED_XML = "malformed/text/xml/none";
  public static final String DIR_ABS_MYDS_STAGED_MALFORMED_XML = "/mydataset/staged/malformed/text/xml/none";

  public static final String DIR_ABS_MYDS_PARTITIONED = "/mydataset/partitioned";
  public static final String DIR_ABS_MYDS_PARTITIONED_CANONICAL = "/mydataset/partitioned/canonical";
  public static final String DIR_REL_MYDS_PARTITIONED_CANONICAL_AVRO = "canonical/avro/binary/none";
  public static final String DIR_ABS_MYDS_PARTITIONED_CANONICAL_AVRO = "/mydataset/partitioned/canonical/avro/binary/none";
  public static final String DIR_ABS_MYDS_PARTITIONED_DUPLICATE = "/mydataset/partitioned/duplicate";
  public static final String DIR_REL_MYDS_PARTITIONED_DUPLICATE_AVRO = "duplicate/avro/binary/none";
  public static final String DIR_ABS_MYDS_PARTITIONED_DUPLICATE_AVRO = "/mydataset/partitioned/duplicate/avro/binary/none";
  public static final String DIR_ABS_MYDS_PARTITIONED_MALFORMED = "/mydataset/partitioned/malformed";
  public static final String DIR_REL_MYDS_PARTITIONED_MALFORMED_CSV = "malformed/text/csv/none";
  public static final String DIR_ABS_MYDS_PARTITIONED_MALFORMED_CSV = "/mydataset/partitioned/malformed/text/csv/none";
  public static final String DIR_REL_MYDS_PARTITIONED_MALFORMED_XML = "malformed/text/xml/none";
  public static final String DIR_ABS_MYDS_PARTITIONED_MALFORMED_XML = "/mydataset/partitioned/malformed/text/xml/none";

  public static final String DIR_ABS_MYDS_CLEANSED = "/mydataset/cleansed";
  public static final String DIR_ABS_MYDS_CLEANSED_CANONICAL = "/mydataset/cleansed/canonical";
  public static final String DIR_REL_MYDS_CLEANSED_CANONICAL_PARQUET = "canonical/parquet/dict/snappy";
  public static final String DIR_ABS_MYDS_CLEANSED_CANONICAL_PARQUET = "/mydataset/cleansed/canonical/parquet/dict/snappy";
  public static final String DIR_ABS_MYDS_CLEANSED_REWRITTEN = "/mydataset/cleansed/rewritten";
  public static final String DIR_REL_MYDS_CLEANSED_REWRITTEN_PARQUET = "rewritten/parquet/dict/snappy";
  public static final String DIR_ABS_MYDS_CLEANSED_REWRITTEN_PARQUET = "/mydataset/cleansed/rewritten/parquet/dict/snappy";
  public static final String DIR_ABS_MYDS_CLEANSED_DUPLICATE = "/mydataset/cleansed/duplicate";
  public static final String DIR_REL_MYDS_CLEANSED_DUPLICATE_PARQUET = "duplicate/parquet/dict/snappy";
  public static final String DIR_ABS_MYDS_CLEANSED_DUPLICATE_PARQUET = "/mydataset/cleansed/duplicate/parquet/dict/snappy";

  public static final String DDL_FILE_BATCH_NAME = "batch_name.ddl";
  public static final String DDL_FILE_BATCH_ID_START_FINISH = "batch_id_start_finish.ddl";

  public static final String DDL_FILE_CREATE_TEXT = "text.ddl";
  public static final String DDL_FILE_CREATE_AVRO = "avro.ddl";
  public static final String DDL_FILE_CREATE_PARQUET = "parquet.ddl";

  public static final String DDL_VAR_FILE = "my.table.file";
  public static final String DDL_VAR_ROWS = "my.table.rows";
  public static final String DDL_VAR_NAME = "my.table.name";
  public static final String DDL_VAR_SERDE = "my.table.serde";
  public static final String DDL_VAR_INPUT = "my.table.input";
  public static final String DDL_VAR_OUTPUT = "my.table.output";
  public static final String DDL_VAR_SCHEMA = "my.table.schema";
  public static final String DDL_VAR_ROOT = "my.table.root";
  public static final String DDL_VAR_LOCATION = "my.table.location";
  public static final String DDL_VAR_PARTITION = "my.table.partition";
  public static final String DDL_VAR_DATABASE = "my.database.name";

  public static final String MODEL_AVRO_FILE = "/avro/model.avsc";

  public static final String CONFIG_INPUT_PATH = "cloudera.input.dir";
  public static final String CONFIG_OUTPUT_PATH = "cloudera.output.dir";

}
