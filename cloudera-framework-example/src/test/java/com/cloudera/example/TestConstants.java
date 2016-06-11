package com.cloudera.example;

/**
 * Test constants
 */
public interface TestConstants extends com.cloudera.framework.testing.TestConstants {

  public static final String DS_DIR = REL_DIR_DATASET;
  public static final String DS_MYDATASET = "mydataset";
  public static final String DSS_MYDATASET_CSV = "csv";
  public static final String DSS_MYDATASET_XML = "xml";

  public static final String DSS_MYDATASET_PRISTINE_SINGLE = "pristine-single";
  public static final String DSS_MYDATASET_PRISTINE_MULTI = "pristine-multi";
  public static final String DSS_MYDATASET_CORRUPT_DIR = "corrupt-dir";
  public static final String DSS_MYDATASET_CORRUPT_FILE = "corrupt-file";
  public static final String DSS_MYDATASET_CORRUPT_STRUCT = "corrupt-struct";
  public static final String DSS_MYDATASET_CORRUPT_TYPE = "corrupt-type";
  public static final String DSS_MYDATASET_DUPLICATE_EVENT = "duplicate-event";
  public static final String DSS_MYDATASET_DUPLICATE_RECORD = "duplicate-record";
  public static final String DSS_MYDATASET_EMPTY_DIR = "empty-dir";
  public static final String DSS_MYDATASET_EMPTY_FILE = "empty-file";

}
