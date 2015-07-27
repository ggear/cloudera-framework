package com.cloudera.example;

import com.cloudera.framework.main.test.BaseTest;

/**
 * Test constants
 */
public interface TestConstants extends Constants {

  public static final String DS_DIR = BaseTest.REL_DIR_DATASET;
  public static final String DS_MYDATASET = "my-dataset";
  public static final String DSS_MYDATASET_TSV = "tsv";
  public static final String DSS_MYDATASET_CSV = "csv";

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
