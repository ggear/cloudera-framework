package com.cloudera.example;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.cloudera.framework.main.test.MiniClusterDfsMrTest;

/**
 * Test dataset cleanse operation
 */
public class MyDatasetCleanseTest extends MiniClusterDfsMrTest implements MyBaseTest {

  @Before
  public void prepareData() throws IllegalArgumentException, IOException {
    copyFromTestClassesDir(DATA_NAME, DATASET_TAB_PATH, DATASET_NAME,
        DATASET_TAB_NAME);
    copyFromTestClassesDir(DATA_NAME, DATASET_COMMA_PATH, DATASET_NAME,
        DATASET_COMMA_NAME);
  }

  @Test
  public void testDriver() throws Exception {
  }

}
