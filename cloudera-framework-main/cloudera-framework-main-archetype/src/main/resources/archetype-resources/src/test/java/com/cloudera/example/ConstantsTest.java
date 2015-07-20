package com.cloudera.example;

import com.cloudera.framework.main.test.BaseTest;

/**
 * Test constants
 */
public interface ConstantsTest extends Constants {

  public static final String DS_DIR = BaseTest.REL_DIR_DATASET;
  public static final String DS_MYDATASET = "my-dataset";
  public static final String DSS_MYDATASET_TAB = "tab-delim";
  public static final String DSS_MYDATASET_COMMA = "comma-delim";
  public static final String DSS_MYDATASET_PRISTINE = "pristine";
  public static final String DSS_MYDATASET_DUPLICATES = "duplicates";
  public static final String DSS_MYDATASET_EMPTYFILES = "empty-files";
  public static final String DSS_MYDATASET_WRONGCOLUMNS = "wrong-columns";
  public static final String DSS_MYDATASET_WRONGTYPES = "wrong-types";

  public static final String DDL_DIR = "/ddl";
  public static final String DDL_TEXT = "table_text.ddl";
  public static final String DDL_AVRO = "table_avro.ddl";
  public static final String DDL_PARQUET = "table_parquet.ddl";
  public static final String DDL_TABLENAME = "my.table.name";
  public static final String DDL_TABLEDELIM = "my.table.delim";
  public static final String DDL_TABLEAVROSCHEMA = "my.table.avroschema";
  public static final String DDL_TABLELOCATION = "my.table.location";

  public static final String SQL_DIR = "/sql";
  public static final String SQL_STATS = "stats.sql";

  public static final String AVRO_DIR = "/avro";
  public static final String AVRO_MODEL = "model.avsc";

}
