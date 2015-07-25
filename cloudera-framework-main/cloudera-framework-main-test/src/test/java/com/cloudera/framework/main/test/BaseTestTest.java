package com.cloudera.framework.main.test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.ImmutableMap;

/**
 * Base system test
 */
@RunWith(value = Parameterized.class)
public class BaseTestTest extends LocalClusterDfsMrTest {

  @Parameters()
  public static Iterable<Object[]> parameters() {
    return Arrays.asList(new Object[][] {
        //
        {
            //
            new String[] { DIR_SOURCE, }, //
            new String[] { DIR_DESTINATION, }, //
            new String[] { null, }, //
            new String[][] { //
                { null }, //
        }, //
            new String[][][] { { //
                //
                { null }, //
            } }, //
            new Map[] { Collections.EMPTY_MAP, }, //
        }, //
        {
            //
            new String[] { DIR_SOURCE, }, //
            new String[] { DIR_DESTINATION, }, //
            new String[] { "dataset-1", }, //
            new String[][] { //
                { null }, //
        }, //
            new String[][][] { { //
                //
                { null }, //
            } }, //
            new Map[] { Collections.EMPTY_MAP, }, //
        }, //
        {
            //
            new String[] { DIR_SOURCE, }, //
            new String[] { DIR_DESTINATION, }, //
            new String[] { "dataset-1", }, //
            new String[][] { //
                { "dataset-1-sub-1" }, //
        }, //
            new String[][][] { { //
                //
                { null }, //
            } }, //
            new Map[] { Collections.EMPTY_MAP, }, //
        }, //
        {
            //
            new String[] { DIR_SOURCE, }, //
            new String[] { DIR_DESTINATION, }, //
            new String[] { "dataset-1", }, //
            new String[][] { //
                { "dataset-1-sub-1" }, //
        }, //
            new String[][][] { { //
                //
                { "dataset-1-sub-1-sub-1" }, //
            } }, //
            new Map[] { Collections.EMPTY_MAP, }, //
        }, //
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
        }, //
            new Map[] {
                //
                ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, 1L)), //
                ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER2, 2L)), //
                ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER3, 3L, Counter.COUNTER4, 4L)), //
        }, //
        }, //
    });
  }

  public static final String COUNTER_GROUP = BaseTestTest.class.getCanonicalName();

  public enum Counter {
    COUNTER1, COUNTER2, COUNTER3, COUNTER4
  }

  public BaseTestTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets,
      String[][][] labels, @SuppressWarnings("rawtypes") Map[] metadata) {
    super(sources, destinations, datasets, subsets, labels, metadata);
  }

  @Override
  public void setupDatasets() throws IllegalArgumentException, IOException {
  }

  private static final String DIR_SOURCE = REL_DIR_CLASSES + "/data";
  private static final String DIR_DESTINATION = "/tmp/data";

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

  private void assertCopyFromLocalDir(int countUpstream, int countDownstream, String sourcePath, String destinationPath,
      String... sourceLabels) throws Exception {
    Assert.assertEquals(countUpstream, listFilesDfs(destinationPath).length);
    Assert.assertTrue(copyFromLocalDir(sourcePath, destinationPath, sourceLabels).length > 0);
    Assert.assertEquals(countDownstream, listFilesDfs(destinationPath).length);
  }

  @Test
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void testAssertCounterEqualsLessThanGreaterThan() {
    assertCounterEquals(metadata[0], metadata[0]);
    assertCounterEquals(
        ImmutableMap.of(COUNTER_GROUP + "1",
            ImmutableMap.of(Counter.COUNTER1, 1L, Counter.COUNTER2, 2L, Counter.COUNTER3, 3L), COUNTER_GROUP + "2",
            ImmutableMap.of(Counter.COUNTER1, 1L, Counter.COUNTER2, 2L)),
        (Map) ImmutableMap.of(COUNTER_GROUP + "1",
            ImmutableMap.of(Counter.COUNTER1, 1L, Counter.COUNTER2, 2L, Counter.COUNTER3, 3L), COUNTER_GROUP + "2",
            ImmutableMap.of(Counter.COUNTER1, 1L, Counter.COUNTER2, 2L)));
    assertCounterLessThan(
        ImmutableMap.of(COUNTER_GROUP + "1",
            ImmutableMap.of(Counter.COUNTER1, 1L, Counter.COUNTER2, 2L, Counter.COUNTER3, 3L), COUNTER_GROUP + "2",
            ImmutableMap.of(Counter.COUNTER1, 1L, Counter.COUNTER2, 2L)),
        (Map) ImmutableMap.of(COUNTER_GROUP + "1",
            ImmutableMap.of(Counter.COUNTER1, 0L, Counter.COUNTER2, 1L, Counter.COUNTER3, 2L), COUNTER_GROUP + "2",
            ImmutableMap.of(Counter.COUNTER1, 0L, Counter.COUNTER2, 1L)));
    assertCounterGreaterThan(
        ImmutableMap.of(COUNTER_GROUP + "1",
            ImmutableMap.of(Counter.COUNTER1, 1L, Counter.COUNTER2, 2L, Counter.COUNTER3, 3L), COUNTER_GROUP + "2",
            ImmutableMap.of(Counter.COUNTER1, 1L, Counter.COUNTER2, 2L)),
        (Map) ImmutableMap.of(COUNTER_GROUP + "1",
            ImmutableMap.of(Counter.COUNTER1, 2L, Counter.COUNTER2, 3L, Counter.COUNTER3, 4L), COUNTER_GROUP + "2",
            ImmutableMap.of(Counter.COUNTER1, 2L, Counter.COUNTER2, 3L)));
    assertCounterEqualsLessThanGreaterThan(ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, 1L)),
        Collections.EMPTY_MAP, Collections.EMPTY_MAP,
        (Map) ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, 1L)));
    assertCounterEqualsLessThanGreaterThan(Collections.EMPTY_MAP,
        ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, 2L)), Collections.EMPTY_MAP,
        (Map) ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, 1L)));
    assertCounterEqualsLessThanGreaterThan(Collections.EMPTY_MAP, Collections.EMPTY_MAP,
        ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, 0L)),
        (Map) ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, 1L)));
    assertCounterEqualsLessThanGreaterThan(ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, 1L)),
        ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, 2L)),
        ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, 0L)),
        (Map) ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, 1L)));
  }

}
