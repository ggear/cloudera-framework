package com.cloudera.example;

/**
 * Constants
 */
public interface Constants {

  public static final String DIR_DS_MYDATASET = "/mydataset";
  public static final String DIR_DS_MYDATASET_CANONICAL = "canonical";
  public static final String DIR_DS_MYDATASET_DUPLICATE = "duplicate";
  public static final String DIR_DS_MYDATASET_MALFORMED = "malformed";
  public static final String DIR_DS_MYDATASET_REWRITTEN = "rewritten";

  public static final String DIR_DS_MYDATASET_RAW = DIR_DS_MYDATASET + "/raw";
  public static final String DIR_DS_MYDATASET_RAW_CANONICAL = DIR_DS_MYDATASET_RAW + "/canonical";
  public static final String DIR_DS_MYDATASET_RAW_CANONICAL_TEXT = DIR_DS_MYDATASET_RAW_CANONICAL + "/text";
  public static final String DIR_DS_MYDATASET_RAW_CANONICAL_TEXT_XML = DIR_DS_MYDATASET_RAW_CANONICAL_TEXT
      + "/xml/none";
  public static final String DIR_DS_MYDATASET_RAW_CANONICAL_TEXT_CSV = DIR_DS_MYDATASET_RAW_CANONICAL_TEXT
      + "/csv/none";

  public static final String DIR_DS_MYDATASET_STAGED = DIR_DS_MYDATASET + "/staged";
  public static final String DIR_DS_MYDATASET_STAGED_CANONICAL = DIR_DS_MYDATASET_STAGED + "/"
      + DIR_DS_MYDATASET_CANONICAL;
  public static final String DIR_DS_MYDATASET_STAGED_MALFORMED = DIR_DS_MYDATASET_STAGED + "/"
      + DIR_DS_MYDATASET_MALFORMED;

  public static final String DIR_DS_MYDATASET_PARTITIONED = DIR_DS_MYDATASET + "/partitioned";
  public static final String DIR_DS_MYDATASET_PARTITIONED_CANONICAL = DIR_DS_MYDATASET_PARTITIONED + "/"
      + DIR_DS_MYDATASET_CANONICAL;
  public static final String DIR_DS_MYDATASET_PARTITIONED_CANONICAL_AVRO_RELATIVE = DIR_DS_MYDATASET_CANONICAL
      + "/avro/binary/none";
  public static final String DIR_DS_MYDATASET_PARTITIONED_CANONICAL_AVRO = DIR_DS_MYDATASET_PARTITIONED + "/"
      + DIR_DS_MYDATASET_PARTITIONED_CANONICAL_AVRO_RELATIVE;
  public static final String DIR_DS_MYDATASET_PARTITIONED_DUPLICATE = DIR_DS_MYDATASET_PARTITIONED + "/"
      + DIR_DS_MYDATASET_DUPLICATE;
  public static final String DIR_DS_MYDATASET_PARTITIONED_DUPLICATE_AVRO_RELATIVE = DIR_DS_MYDATASET_DUPLICATE
      + "/avro/binary/none";
  public static final String DIR_DS_MYDATASET_PARTITIONED_DUPLICATE_AVRO = DIR_DS_MYDATASET_PARTITIONED + "/"
      + DIR_DS_MYDATASET_PARTITIONED_DUPLICATE_AVRO_RELATIVE;
  public static final String DIR_DS_MYDATASET_PARTITIONED_MALFORMED = DIR_DS_MYDATASET_PARTITIONED + "/"
      + DIR_DS_MYDATASET_MALFORMED;
  public static final String DIR_DS_MYDATASET_PARTITIONED_MALFORMED_RELATIVE = DIR_DS_MYDATASET_MALFORMED
      + "/sequence/text/none";

  public static final String DIR_DS_MYDATASET_PROCESSED = DIR_DS_MYDATASET + "/processed";
  public static final String DIR_DS_MYDATASET_PROCESSED_CANONICAL = DIR_DS_MYDATASET_PROCESSED + "/"
      + DIR_DS_MYDATASET_CANONICAL;
  public static final String DIR_DS_MYDATASET_PROCESSED_CANONICAL_PARQUET_RELATIVE = DIR_DS_MYDATASET_CANONICAL
      + "/parquet/dict/snappy";
  public static final String DIR_DS_MYDATASET_PROCESSED_CANONICAL_PARQUET = DIR_DS_MYDATASET_PROCESSED + "/"
      + DIR_DS_MYDATASET_PROCESSED_CANONICAL_PARQUET_RELATIVE;
  public static final String DIR_DS_MYDATASET_PROCESSED_DUPLICATE = DIR_DS_MYDATASET_PROCESSED + "/"
      + DIR_DS_MYDATASET_DUPLICATE;
  public static final String DIR_DS_MYDATASET_PROCESSED_DUPLICATE_PARQUET_RELATIVE = DIR_DS_MYDATASET_DUPLICATE
      + "/parquet/dict/snappy";
  public static final String DIR_DS_MYDATASET_PROCESSED_DUPLICATE_PARQUET = DIR_DS_MYDATASET_PROCESSED + "/"
      + DIR_DS_MYDATASET_PROCESSED_DUPLICATE_PARQUET_RELATIVE;
  public static final String DIR_DS_MYDATASET_PROCESSED_MALFORMED = DIR_DS_MYDATASET_PROCESSED + "/"
      + DIR_DS_MYDATASET_MALFORMED;
  public static final String DIR_DS_MYDATASET_PROCESSED_MALFORMED_RELATIVE = DIR_DS_MYDATASET_MALFORMED
      + "/avro/binary/none";

  public static final String DDL_DIR = "/ddl/hive";

  public static final String DDL_FILE_BATCH_NAME = "table_create_batch_name.ddl";
  public static final String DDL_FILE_BATCH_ID_START_FINISH = "table_create_batch_id_start_finish.ddl";

  public static final String DDL_FILE_CREATE_AVRO = "table_create_avro.ddl";
  public static final String DDL_FILE_CREATE_PARQUET = "table_create_parquet.ddl";

  public static final String DDL_VAR_FILE = "my.table.file";
  public static final String DDL_VAR_ROWS = "my.table.rows";
  public static final String DDL_VAR_NAME = "my.table.name";
  public static final String DDL_VAR_SERDE = "my.table.serde";
  public static final String DDL_VAR_INPUT = "my.table.input";
  public static final String DDL_VAR_OUTPUT = "my.table.output";
  public static final String DDL_VAR_SCHEMA = "my.table.schema";
  public static final String DDL_VAR_LOCATION = "my.table.location";
  public static final String DDL_VAR_PARTITION = "my.table.partition";

  public static final String MODEL_AVRO_FILE = "/cfg/avro/model.avsc";

  public static final String CONFIG_INPUT_PATH = "cloudera.input.dir";
  public static final String CONFIG_OUTPUT_PATH = "cloudera.output.dir";

}
