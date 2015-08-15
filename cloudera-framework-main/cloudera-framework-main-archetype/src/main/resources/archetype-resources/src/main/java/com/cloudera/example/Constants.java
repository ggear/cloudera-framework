package com.cloudera.example;

/**
 * Constants
 */
public interface Constants {

  public static final String DIR_DS_MYDATASET = "/mydataset";
  public static final String DIR_DS_MYDATASET_CLEANSED = "cleansed";
  public static final String DIR_DS_MYDATASET_DUPLICATE = "duplicate";
  public static final String DIR_DS_MYDATASET_MALFORMED = "malformed";
  public static final String DIR_DS_MYDATASET_PARTITIONED = "partitioned";

  public static final String DIR_DS_MYDATASET_RAW = DIR_DS_MYDATASET + "/raw";
  public static final String DIR_DS_MYDATASET_RAW_SOURCE = DIR_DS_MYDATASET_RAW + "/source";
  public static final String DIR_DS_MYDATASET_RAW_SOURCE_TEXT = DIR_DS_MYDATASET_RAW_SOURCE + "/text";
  public static final String DIR_DS_MYDATASET_RAW_SOURCE_TEXT_XML = DIR_DS_MYDATASET_RAW_SOURCE_TEXT + "/xml/none";
  public static final String DIR_DS_MYDATASET_RAW_SOURCE_TEXT_CSV = DIR_DS_MYDATASET_RAW_SOURCE_TEXT + "/csv/none";

  public static final String DIR_DS_MYDATASET_STAGED = DIR_DS_MYDATASET + "/staged";
  public static final String DIR_DS_MYDATASET_STAGED_PARTITIONED = DIR_DS_MYDATASET_STAGED + "/"
      + DIR_DS_MYDATASET_PARTITIONED;
  public static final String DIR_DS_MYDATASET_STAGED_MALFORMED = DIR_DS_MYDATASET_STAGED + "/"
      + DIR_DS_MYDATASET_MALFORMED;

  public static final String DIR_DS_MYDATASET_PROCESSED = DIR_DS_MYDATASET + "/processed";
  public static final String DIR_DS_MYDATASET_PROCESSED_CLEANSED = DIR_DS_MYDATASET_PROCESSED + "/"
      + DIR_DS_MYDATASET_CLEANSED;
  public static final String DIR_DS_MYDATASET_PROCESSED_CLEANSED_AVRO_RELATIVE = DIR_DS_MYDATASET_CLEANSED
      + "/avro/binary/none";
  public static final String DIR_DS_MYDATASET_PROCESSED_CLEANSED_AVRO = DIR_DS_MYDATASET_PROCESSED + "/"
      + DIR_DS_MYDATASET_PROCESSED_CLEANSED_AVRO_RELATIVE;
  public static final String DIR_DS_MYDATASET_PROCESSED_DUPLICATE = DIR_DS_MYDATASET_PROCESSED + "/"
      + DIR_DS_MYDATASET_DUPLICATE;
  public static final String DIR_DS_MYDATASET_PROCESSED_DUPLICATE_AVRO_RELATIVE = DIR_DS_MYDATASET_DUPLICATE
      + "/avro/binary/none";
  public static final String DIR_DS_MYDATASET_PROCESSED_DUPLICATE_AVRO = DIR_DS_MYDATASET_PROCESSED + "/"
      + DIR_DS_MYDATASET_PROCESSED_DUPLICATE_AVRO_RELATIVE;
  public static final String DIR_DS_MYDATASET_PROCESSED_MALFORMED = DIR_DS_MYDATASET_PROCESSED + "/"
      + DIR_DS_MYDATASET_MALFORMED;
  public static final String DIR_DS_MYDATASET_PROCESSED_MALFORMED_RELATIVE = DIR_DS_MYDATASET_MALFORMED
      + "/avro/binary/none";

  public static final String DDL_DIR = "/ddl/hive";
  public static final String DDL_FILE_BATCH_NAME = "table_create_batch_name.ddl";
  public static final String DDL_FILE_BATCH_ID_START_FINISH = "table_create_batch_id_start_finish.ddl";
  public static final String DDL_FILE_BATCH_YEAR_MONTH = "table_create_batch_year_month.ddl";
  public static final String DDL_VAR_FILE = "my.table.file";
  public static final String DDL_VAR_ROWS = "my.table.rows";
  public static final String DDL_VAR_NAME = "my.table.name";
  public static final String DDL_VAR_FORMAT = "my.table.format";
  public static final String DDL_VAR_SCHEMA = "my.table.schema";
  public static final String DDL_VAR_LOCATION = "my.table.location";

  public static final String MODEL_AVRO_FILE = "/cfg/avro/model.avsc";

  public static final String CONFIG_INPUT_PATH = "cloudera.input.dir";
  public static final String CONFIG_OUTPUT_PATH = "cloudera.output.dir";

}
