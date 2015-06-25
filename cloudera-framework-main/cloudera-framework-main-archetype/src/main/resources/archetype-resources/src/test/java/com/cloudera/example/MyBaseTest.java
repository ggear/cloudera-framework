package com.cloudera.example;

/**
 * Base test for all unit-tests
 */
public interface MyBaseTest {

  public static final String DATA_NAME = "data";
  public static final String DATASET_NAME = "my-dataset";
  public static final String DATASET_PATH = "/" + DATASET_NAME;
  public static final String DATASET_TAB_NAME = "tab-delim";
  public static final String DATASET_TAB_PATH = DATASET_PATH + "/raw/"
      + DATASET_TAB_NAME;
  public static final String DATASET_COMMA_NAME = "comma-delim";
  public static final String DATASET_COMMA_PATH = DATASET_PATH + "/raw/"
      + DATASET_COMMA_NAME;

}
