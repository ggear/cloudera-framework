package com.cloudera.example;

import com.cloudera.framework.main.test.BaseTest;

/**
 * Test constants
 */
public interface MyDataset {

  public static final String DS_MYDATASET = "my-dataset";
  public static final String DSS_MYDATASET_TAB = "tab-delim";
  public static final String DSS_MYDATASET_COMMA = "comma-delim";
  public static final String DSS_MYDATASET_CLEANSED = "cleansed";

  public static final String DIR_DS = BaseTest.REL_DIR_DATASET;
  public static final String DIR_DS_MYDATASET = "/" + DS_MYDATASET;
  public static final String DIR_DS_MYDATASET_TAB = DIR_DS_MYDATASET + "/raw/" + DSS_MYDATASET_TAB;
  public static final String DIR_DS_MYDATASET_COMMA = DIR_DS_MYDATASET + "/raw/" + DSS_MYDATASET_COMMA;
  public static final String DIR_DS_MYDATASET_CLEANSED = DIR_DS_MYDATASET + "/processed/" + DSS_MYDATASET_CLEANSED;

}
