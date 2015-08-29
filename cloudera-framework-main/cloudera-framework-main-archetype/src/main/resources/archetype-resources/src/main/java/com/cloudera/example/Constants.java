package com.cloudera.example;

/**
 * Constants
 */
public interface Constants {

  public static final String DIR_REL_MYDS = "mydataset";
  public static final String DIR_ABS_MYDS = "/mydataset";

  public static final String DIR_REL_MYDS_RAW = "raw";
  public static final String DIR_REL_MYDS_STAGED = "staged";
  public static final String DIR_REL_MYDS_PARTITIONED = "partitioned";
  public static final String DIR_REL_MYDS_PROCESSED = "processed";

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

  public static final String DIR_ABS_MYDS_PROCESSED = "/mydataset/processed";
  public static final String DIR_ABS_MYDS_PROCESSED_CANONICAL = "/mydataset/processed/canonical";
  public static final String DIR_REL_MYDS_PROCESSED_CANONICAL_PARQUET = "canonical/parquet/dict/snappy";
  public static final String DIR_ABS_MYDS_PROCESSED_CANONICAL_PARQUET = "/mydataset/processed/canonical/parquet/dict/snappy";
  public static final String DIR_ABS_MYDS_PROCESSED_REWRITTEN = "/mydataset/processed/rewritten";
  public static final String DIR_REL_MYDS_PROCESSED_REWRITTEN_PARQUET = "rewritten/parquet/dict/snappy";
  public static final String DIR_ABS_MYDS_PROCESSED_REWRITTEN_PARQUET = "/mydataset/processed/rewritten/parquet/dict/snappy";
  public static final String DIR_ABS_MYDS_PROCESSED_DUPLICATE = "/mydataset/processed/duplicate";
  public static final String DIR_REL_MYDS_PROCESSED_DUPLICATE_PARQUET = "duplicate/parquet/dict/snappy";
  public static final String DIR_ABS_MYDS_PROCESSED_DUPLICATE_PARQUET = "/mydataset/processed/duplicate/parquet/dict/snappy";

  public static final String DDL_DIR = "/ddl/hive";

  public static final String DDL_FILE_BATCH_NAME = "table_create_batch_name.ddl";
  public static final String DDL_FILE_BATCH_ID_START_FINISH = "table_create_batch_id_start_finish.ddl";

  public static final String DDL_FILE_CREATE_TEXT = "table_create_text.ddl";
  public static final String DDL_FILE_CREATE_AVRO = "table_create_avro.ddl";
  public static final String DDL_FILE_CREATE_PARQUET = "table_create_parquet.ddl";

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

  public static final String MODEL_AVRO_FILE = "/cfg/avro/model.avsc";

  public static final String CONFIG_INPUT_PATH = "cloudera.input.dir";
  public static final String CONFIG_OUTPUT_PATH = "cloudera.output.dir";

}
