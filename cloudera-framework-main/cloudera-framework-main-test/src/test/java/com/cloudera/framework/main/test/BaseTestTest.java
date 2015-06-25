package com.cloudera.framework.main.test;

import org.junit.Assert;
import org.junit.Test;

/**
 * Base test unit tests
 */
public class BaseTestTest extends LocalClusterDfsMrTest {

  private static final String DIR_SOURCE = REL_DIR_CLASSES + "/data";
  private static final String DIR_DESTINATION = "/tmp/data";

  @Test
  public void testCopyFromTestClassesDirAll() throws Exception {
    assertCopyFromTestClassesDir(0, 4, DIR_SOURCE, DIR_DESTINATION);
    assertCopyFromTestClassesDir(4, 4, DIR_SOURCE, DIR_DESTINATION);
  }

  @Test
  public void testCopyFromTestClassesDirSubset() throws Exception {
    assertCopyFromTestClassesDir(0, 2, DIR_SOURCE, DIR_DESTINATION, "dataset-1");
    assertCopyFromTestClassesDir(2, 3, DIR_SOURCE, DIR_DESTINATION, "dataset-2");
    assertCopyFromTestClassesDir(3, 4, DIR_SOURCE, DIR_DESTINATION, "dataset-3");
  }

  @Test
  public void testCopyFromTestClassesDirDirToFile() throws Exception {
    assertCopyFromTestClassesDir(0, 2, DIR_SOURCE, DIR_DESTINATION,
        "dataset-2", "dataset-2-sub-2", "dataset-2-sub-2-sub-2");
    assertCopyFromTestClassesDir(2, 1, DIR_SOURCE, DIR_DESTINATION,
        "dataset-1", "dataset-1-sub-1", "dataset-1-sub-1-sub-2");
  }

  @Test
  public void testCopyFromTestClassesDirFileToDir() throws Exception {
    assertCopyFromTestClassesDir(0, 1, DIR_SOURCE, DIR_DESTINATION,
        "dataset-1", "dataset-1-sub-1", "dataset-1-sub-1-sub-2");
    assertCopyFromTestClassesDir(1, 2, DIR_SOURCE, DIR_DESTINATION,
        "dataset-2", "dataset-2-sub-2", "dataset-2-sub-2-sub-2");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCopyFromTestClassesDirBadLabel() throws Exception {
    assertCopyFromTestClassesDir(0, 2, DIR_SOURCE, DIR_DESTINATION,
        "eroneous-dataset-2");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCopyFromTestClassesDirBadLabels() throws Exception {
    assertCopyFromTestClassesDir(0, 2, DIR_SOURCE, DIR_DESTINATION,
        "eroneous-dataset-2", "eroneous-dataset-2-sub-1");
  }

  private void assertCopyFromTestClassesDir(int countUpstream,
      int countDownstream, String sourcePath, String destinationPath,
      String... sourceLabels) throws Exception {
    Assert.assertEquals(countUpstream, listFilesDfs(destinationPath).size());
    Assert.assertTrue(countDownstream <= copyFromLocalDir(sourcePath,
        destinationPath, sourceLabels).size());
    Assert.assertEquals(countDownstream, listFilesDfs(destinationPath).size());
  }

}
