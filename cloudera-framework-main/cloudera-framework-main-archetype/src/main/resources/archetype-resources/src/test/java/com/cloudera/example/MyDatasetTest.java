package com.cloudera.example;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.framework.main.test.LocalClusterDfsMrTest;

public class MyDatasetTest extends LocalClusterDfsMrTest implements MyBaseTest {

  @Before
  public void prepareData() throws IllegalArgumentException, IOException {
    copyFromTestClassesDir(DATA_NAME, DATASET_TAB_PATH, DATASET_NAME,
        DATASET_TAB_NAME);
    copyFromTestClassesDir(DATA_NAME, DATASET_COMMA_PATH, DATASET_NAME,
        DATASET_COMMA_NAME);
  }

  @Test
  public void testDataAvailable() throws Exception {
    Assert.assertTrue(getFileSystem()
        .exists(new Path(getPathDfs(DATASET_PATH))));
  }

}
