package com.cloudera.example;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.cloudera.framework.main.test.MiniClusterDfsMrTest;

/**
 * Test dataset cleanse operation
 */
public class MyDatasetCleanseTest extends MiniClusterDfsMrTest implements
    MyBaseTest {

  @Before
  public void prepareData() throws IllegalArgumentException, IOException {
    copyFromLocalDir(REL_DIR_SOURCE, REL_DIR_DATASET_TAB, DIR_DATASET,
        DIR_DATASET_TAB);
    copyFromLocalDir(REL_DIR_SOURCE, REL_DIR_DATASET_COMMA, DIR_DATASET,
        DIR_DATASET_COMMA);
  }

  @Test
  public void testDriver() throws Exception {
  }

}
