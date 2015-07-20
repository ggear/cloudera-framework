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

}
