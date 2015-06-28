package com.cloudera.framework.main.test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Base test unit tests
 */
@RunWith(value = Parameterized.class)
public class BaseTestTest extends LocalClusterDfsMrTest {

  private static final String DIR_SOURCE = REL_DIR_CLASSES + "/data";
  private static final String DIR_DESTINATION = "/tmp/data";

  private String[] sources;
  private String[] destinations;
  private String[] datasets;
  private String[][] subsets;
  private String[][][] labels;

  public BaseTestTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets,
      String[][][] labels) {
    this.sources = sources;
    this.destinations = destinations;
    this.datasets = datasets;
    this.subsets = subsets;
    this.labels = labels;
  }

  @Parameters()
  public static Iterable<Object[]> data1() {
    return Arrays.asList(new Object[][] {
        //
        {
            //
            new String[] { DIR_SOURCE, }, //
            new String[] { DIR_DESTINATION, }, //
            new String[] { null, }, //
            new String[][] {//
            { null }, //
            }, //
            new String[][][] { { //
            //
            { null }, //
            } } }, //
        {
            //
            new String[] { DIR_SOURCE, }, //
            new String[] { DIR_DESTINATION, }, //
            new String[] { "dataset-1", }, //
            new String[][] {//
            { null }, //
            }, //
            new String[][][] { { //
            //
            { null }, //
            } } }, //
        {
            //
            new String[] { DIR_SOURCE, }, //
            new String[] { DIR_DESTINATION, }, //
            new String[] { "dataset-1", }, //
            new String[][] {//
            { "dataset-1-sub-1" }, //
            }, //
            new String[][][] { { //
            //
            { null }, //
            } } }, //
        {
            //
            new String[] { DIR_SOURCE, }, //
            new String[] { DIR_DESTINATION, }, //
            new String[] { "dataset-1", }, //
            new String[][] {//
            { "dataset-1-sub-1" }, //
            }, //
            new String[][][] { { //
            //
            { "dataset-1-sub-1-sub-1" }, //
            } } }, //
        {
            //
            new String[] {
                //
                DIR_SOURCE, DIR_SOURCE, }, //
            new String[] {
                //
                DIR_DESTINATION + "/one", DIR_DESTINATION + "/two", }, //
            new String[] {
                //
                "dataset-1", "dataset-1" }, //
            new String[][] {
                //
                { "dataset-1-sub-1" }, //
                { "dataset-1-sub-1" }, //
            }, //
            new String[][][] {
                //
                { { "dataset-1-sub-1-sub-1" }, }, //
                { { "dataset-1-sub-1-sub-1" }, }, //
            } }, //
    });
  }

  @Test
  public void testCopyFromTestClassesDirAll() throws Exception {
    assertCopyFromLocalDir(0, 4, DIR_SOURCE, DIR_DESTINATION);
    assertCopyFromLocalDir(4, 4, DIR_SOURCE, DIR_DESTINATION);
  }

  @Test
  public void testCopyFromTestClassesDirSubset() throws Exception {
    assertCopyFromLocalDir(0, 2, DIR_SOURCE, DIR_DESTINATION, "dataset-1");
    assertCopyFromLocalDir(2, 3, DIR_SOURCE, DIR_DESTINATION, "dataset-2");
    assertCopyFromLocalDir(3, 4, DIR_SOURCE, DIR_DESTINATION, "dataset-3");
  }

  @Test
  public void testCopyFromTestClassesDirDirToFile() throws Exception {
    assertCopyFromLocalDir(0, 2, DIR_SOURCE, DIR_DESTINATION, "dataset-2", "dataset-2-sub-2", "dataset-2-sub-2-sub-2");
    assertCopyFromLocalDir(2, 1, DIR_SOURCE, DIR_DESTINATION, "dataset-1", "dataset-1-sub-1", "dataset-1-sub-1-sub-2");
  }

  @Test
  public void testCopyFromTestClassesDirFileToDir() throws Exception {
    assertCopyFromLocalDir(0, 1, DIR_SOURCE, DIR_DESTINATION, "dataset-1", "dataset-1-sub-1", "dataset-1-sub-1-sub-2");
    assertCopyFromLocalDir(1, 2, DIR_SOURCE, DIR_DESTINATION, "dataset-2", "dataset-2-sub-2", "dataset-2-sub-2-sub-2");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCopyFromTestClassesDirBadLabel() throws Exception {
    assertCopyFromLocalDir(0, 2, DIR_SOURCE, DIR_DESTINATION, "eroneous-dataset-2");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCopyFromTestClassesDirBadLabels() throws Exception {
    assertCopyFromLocalDir(0, 2, DIR_SOURCE, DIR_DESTINATION, "eroneous-dataset-2", "eroneous-dataset-2-sub-1");
  }

  @Test
  public void testMapFilesLocalAndListFilesLocal() throws Exception {
    Map<String, Map<String, Map<String, List<File>>>> files = mapFilesLocal(DIR_SOURCE);
    Assert.assertEquals(3, files.size());
    for (String dataset : files.keySet()) {
      Assert.assertArrayEquals(copyFromLocalDir(DIR_SOURCE, DIR_DESTINATION, dataset),
          listFilesLocal(DIR_SOURCE, dataset));
      for (String subset : files.get(dataset).keySet()) {
        Assert.assertArrayEquals(copyFromLocalDir(DIR_SOURCE, DIR_DESTINATION, dataset, subset),
            listFilesLocal(DIR_SOURCE, dataset, subset));
        for (String label : files.get(dataset).get(subset).keySet()) {
          Assert.assertArrayEquals(copyFromLocalDir(DIR_SOURCE, DIR_DESTINATION, dataset, subset, label),
              listFilesLocal(DIR_SOURCE, dataset, subset, label));
          Assert.assertEquals(files.get(dataset).get(subset).get(label),
              mapFilesLocal(DIR_SOURCE, dataset, subset, label).get(dataset).get(subset).get(label));
        }
      }
    }
  }

  @Test
  public void testCopyFromLocalDirParamaterised() throws IllegalArgumentException, IOException {
    copyFromLocalDir(sources, destinations, datasets, subsets, labels);
  }

  private void assertCopyFromLocalDir(int countUpstream, int countDownstream, String sourcePath,
      String destinationPath, String... sourceLabels) throws Exception {
    Assert.assertEquals(countUpstream, listFilesDfs(destinationPath).length);
    Assert.assertTrue(copyFromLocalDir(sourcePath, destinationPath, sourceLabels).length > 0);
    Assert.assertEquals(countDownstream, listFilesDfs(destinationPath).length);
  }

}
