package com.cloudera.example;

/**
 * Constants
 */
public interface Constants {

  public static final String DIR_DS_MYDATASET = "/my-dataset";
  public static final String DIR_DS_MYDATASET_RAW = DIR_DS_MYDATASET + "/raw";
  public static final String DIR_DS_MYDATASET_RAW_SOURCE_TEXT = DIR_DS_MYDATASET_RAW + "/source/text";
  public static final String DIR_DS_MYDATASET_RAW_SOURCE_TEXT_TSV = DIR_DS_MYDATASET_RAW_SOURCE_TEXT + "/tsv/none";
  public static final String DIR_DS_MYDATASET_RAW_SOURCE_TEXT_CSV = DIR_DS_MYDATASET_RAW_SOURCE_TEXT + "/csv/none";
  public static final String DIR_DS_MYDATASET_PROCESSED = DIR_DS_MYDATASET + "/processed";
  public static final String DIR_DS_MYDATASET_CLEANSED = "cleansed";
  public static final String DIR_DS_MYDATASET_PROCESSED_CLEANSED = DIR_DS_MYDATASET_PROCESSED + "/"
      + DIR_DS_MYDATASET_CLEANSED;
  public static final String DIR_DS_MYDATASET_PROCESSED_CLEANSED_AVRO_RELATIVE = DIR_DS_MYDATASET_CLEANSED
      + "/avro/binary/none";
  public static final String DIR_DS_MYDATASET_PROCESSED_CLEANSED_AVRO = DIR_DS_MYDATASET_PROCESSED + "/"
      + DIR_DS_MYDATASET_PROCESSED_CLEANSED_AVRO_RELATIVE;
  public static final String DIR_DS_MYDATASET_DUPLICATE = "duplicate";
  public static final String DIR_DS_MYDATASET_PROCESSED_DUPLICATE = DIR_DS_MYDATASET_PROCESSED + "/"
      + DIR_DS_MYDATASET_DUPLICATE;
  public static final String DIR_DS_MYDATASET_PROCESSED_DUPLICATE_AVRO_RELATIVE = DIR_DS_MYDATASET_DUPLICATE
      + "/avro/binary/none";
  public static final String DIR_DS_MYDATASET_PROCESSED_DUPLICATE_AVRO = DIR_DS_MYDATASET_PROCESSED + "/"
      + DIR_DS_MYDATASET_PROCESSED_DUPLICATE_AVRO_RELATIVE;
  public static final String DIR_DS_MYDATASET_MALFORMED = "malformed";
  public static final String DIR_DS_MYDATASET_PROCESSED_MALFORMED = DIR_DS_MYDATASET_PROCESSED + "/"
      + DIR_DS_MYDATASET_MALFORMED;
  public static final String DIR_DS_MYDATASET_PROCESSED_MALFORMED_RELATIVE = DIR_DS_MYDATASET_MALFORMED
      + "/avro/binary/none";

  public static final String DDL_DIR = "/ddl/hive";
  public static final String DDL_FILE_TEXT = "table_text.ddl";
  public static final String DDL_FILE_AVRO = "table_avro.ddl";
  public static final String DDL_FILE_PARQUET = "table_parquet.ddl";
  public static final String DDL_FILE = "my.table.file";
  public static final String DDL_ROWS = "my.table.rows";
  public static final String DDL_TABLENAME = "my.table.name";
  public static final String DDL_TABLEDELIM = "my.table.delim";
  public static final String DDL_TABLEAVROSCHEMA = "my.table.avroschema";
  public static final String DDL_TABLELOCATION = "my.table.location";

  public static final String MODEL_AVRO = "/cfg/avro/model.avsc";

}
