package com.cloudera.example;

import java.io.IOException;

import org.junit.Assert;
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
    Assert.assertTrue(copyFromLocalDir(REL_DIR_SOURCE, REL_DIR_DATASET_TAB,
        DIR_DATASET, DIR_DATASET_TAB).length > 0);
    Assert.assertTrue(copyFromLocalDir(REL_DIR_SOURCE, REL_DIR_DATASET_COMMA,
        DIR_DATASET, DIR_DATASET_COMMA).length > 0);
  }

  @Test
  public void testDriver() throws Exception {
  }

}
