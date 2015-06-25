package com.cloudera.example;

import com.cloudera.framework.main.test.BaseTest;

/**
 * Base test for all unit-tests
 */
public interface MyBaseTest {

  public static final String DIR_DATASET = "my-dataset";
  public static final String DIR_DATASET_TAB = "tab-delim";
  public static final String DIR_DATASET_COMMA = "comma-delim";

  public static final String REL_DIR_SOURCE = BaseTest.REL_DIR_CLASSES
      + "/data";
  public static final String REL_DIR_DESTINATION = "/" + DIR_DATASET;
  public static final String REL_DIR_DATASET_TAB = REL_DIR_DESTINATION
      + "/raw/" + DIR_DATASET_TAB;
  public static final String REL_DIR_DATASET_COMMA = REL_DIR_DESTINATION
      + "/raw/" + DIR_DATASET_COMMA;

}
