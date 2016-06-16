package com.cloudera.framework.testing.server.tests;

import static com.cloudera.framework.testing.Assert.assertCounterEquals;
import static com.cloudera.framework.testing.Assert.assertCounterEqualsLessThanGreaterThan;
import static com.cloudera.framework.testing.Assert.assertCounterGreaterThan;
import static com.cloudera.framework.testing.Assert.assertCounterLessThan;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.MrServer;
import com.google.common.collect.ImmutableMap;

public abstract class TestDfsServer implements TestConstants {

  public abstract DfsServer getDfsServer();

  public static final String COUNTER_GROUP = TestDfsServer.class.getCanonicalName();

  public enum Counter {
    COUNTER1, COUNTER2, COUNTER3, COUNTER4
  }

  @Test
  public void testGetDfsServer() {
    assertNotNull(getDfsServer());
    assertTrue(getDfsServer().isStarted());
  }

  private static final String DIR_SOURCE = REL_DIR_CLASSES + "/data";
  private static final String DIR_DESTINATION = "/tmp/data";

  /**
   * Test DFS mkdir and file touch
   *
   * @throws IOException
   */
  @Test
  public void testDfsMkDir() throws Exception {
    assertFalse(getDfsServer().getFileSystem().exists(getDfsServer().getPath("/some_dir/some_file")));
    assertTrue(getDfsServer().getFileSystem().mkdirs(getDfsServer().getPath("/some_dir")));
    assertTrue(getDfsServer().getFileSystem().createNewFile(getDfsServer().getPath("/some_dir/some_file")));
    assertTrue(getDfsServer().getFileSystem().exists(getDfsServer().getPath("/some_dir/some_file")));
  }

  /**
   * Test DFS is clean
   *
   * @throws IOException
   */
  @Test
  public void testDfsClean() throws IOException {
    assertFalse(new File(getDfsServer().getPath("/some_dir/some_file").toString()).exists());
  }

  /**
   * Test DFS path generation
   *
   * @throws Exception
   */
  @Test
  public void testDfsGetPath() throws Exception {
    String regexScheme = getDfsServer().getRuntime().equals(DfsServer.Runtime.LOCAL_FS) ? ABS_DIR_DFS_LOCAL : "";
    String regexSchemeRoot = getDfsServer().getRuntime().equals(DfsServer.Runtime.LOCAL_FS) ? ABS_DIR_DFS_LOCAL : "/";
    assertEquals(regexSchemeRoot, getDfsServer().getPath("").toString());
    assertEquals(regexSchemeRoot, getDfsServer().getPath("/").toString());
    assertEquals(regexSchemeRoot, getDfsServer().getPath("//").toString());
    assertEquals(regexScheme + "/tmp", getDfsServer().getPath("tmp").toString());
    assertEquals(regexScheme + "/tmp", getDfsServer().getPath("/tmp").toString());
    assertEquals(regexScheme + "/tmp", getDfsServer().getPath("//tmp").toString());
    assertEquals(regexScheme + "/tmp", getDfsServer().getPath("///tmp").toString());
    assertEquals(regexScheme + "/tmp", getDfsServer().getPath("///tmp/").toString());
    assertEquals(regexScheme + "/tmp/tmp", getDfsServer().getPath("///tmp//tmp").toString());
    assertEquals(regexScheme + "/tmp/tmp", getDfsServer().getPath("///tmp//tmp/").toString());
    assertEquals(regexScheme + "/tmp/tmp", getDfsServer().getPath("///tmp//tmp//").toString());
    assertTrue(getDfsServer().getFileSystem().exists(getDfsServer().getPath("")));
    assertTrue(getDfsServer().getFileSystem().exists(getDfsServer().getPath("/")));
    assertTrue(getDfsServer().getFileSystem().exists(getDfsServer().getPath("///tmp/")));
  }

  /**
   * Test DFS path generation
   *
   * @throws Exception
   */
  @Test
  public void testDfsGetPathUri() throws Exception {
    String regexScheme = getDfsServer().getRuntime().equals(DfsServer.Runtime.LOCAL_FS) ? "file:/.*" + DIR_DFS_LOCAL
        : "hdfs://localhost:[0-9]+";
    String regexSchemeRoot = getDfsServer().getRuntime().equals(DfsServer.Runtime.LOCAL_FS) ? "file:/.*" + DIR_DFS_LOCAL
        : "hdfs://localhost:[0-9]+/";
    assertTrue(getDfsServer().getPathUri("").matches(regexSchemeRoot));
    assertTrue(getDfsServer().getPathUri("/").matches(regexSchemeRoot));
    assertTrue(getDfsServer().getPathUri("//").matches(regexSchemeRoot));
    assertTrue(getDfsServer().getPathUri("tmp").matches(regexScheme + "/tmp"));
    assertTrue(getDfsServer().getPathUri("/tmp").matches(regexScheme + "/tmp"));
    assertTrue(getDfsServer().getPathUri("//tmp").matches(regexScheme + "/tmp"));
    assertTrue(getDfsServer().getPathUri("///tmp").matches(regexScheme + "/tmp"));
    assertTrue(getDfsServer().getPathUri("///tmp/").matches(regexScheme + "/tmp"));
    assertTrue(getDfsServer().getPathUri("///tmp//tmp").matches(regexScheme + "/tmp/tmp"));
    assertTrue(getDfsServer().getPathUri("///tmp//tmp/").matches(regexScheme + "/tmp/tmp"));
    assertTrue(getDfsServer().getPathUri("///tmp//tmp//").matches(regexScheme + "/tmp/tmp"));
    assertTrue(getDfsServer().getFileSystem().exists(new Path(getDfsServer().getPathUri(""))));
    assertTrue(getDfsServer().getFileSystem().exists(new Path(getDfsServer().getPathUri("/"))));
    assertTrue(getDfsServer().getFileSystem().exists(new Path(getDfsServer().getPathUri("///tmp/"))));
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
    Map<String, Map<String, Map<String, List<File>>>> files = DfsServer.mapFilesLocal(DIR_SOURCE);
    assertEquals(3, files.size());
    for (String dataset : files.keySet()) {
      assertArrayEquals(getDfsServer().copyFromLocalDir(DIR_SOURCE, DIR_DESTINATION, dataset),
          DfsServer.listFilesLocal(DIR_SOURCE, dataset));
      for (String subset : files.get(dataset).keySet()) {
        assertArrayEquals(getDfsServer().copyFromLocalDir(DIR_SOURCE, DIR_DESTINATION, dataset, subset),
            DfsServer.listFilesLocal(DIR_SOURCE, dataset, subset));
        for (String label : files.get(dataset).get(subset).keySet()) {
          assertArrayEquals(getDfsServer().copyFromLocalDir(DIR_SOURCE, DIR_DESTINATION, dataset, subset, label),
              DfsServer.listFilesLocal(DIR_SOURCE, dataset, subset, label));
          assertEquals(files.get(dataset).get(subset).get(label),
              DfsServer.mapFilesLocal(DIR_SOURCE, dataset, subset, label).get(dataset).get(subset).get(label));
        }
      }
    }
  }

  @Test
  public void testAssertCounterEqualsLessThanGreaterThan() {
    assertCounterEquals(
        ImmutableMap.of(COUNTER_GROUP + "1", ImmutableMap.of(Counter.COUNTER1, 1L, Counter.COUNTER2, 2L, Counter.COUNTER3, 3L),
            COUNTER_GROUP + "2", ImmutableMap.of(Counter.COUNTER1, 1L, Counter.COUNTER2, 2L)),
        ImmutableMap.of(COUNTER_GROUP + "1", ImmutableMap.of(Counter.COUNTER1, 1L, Counter.COUNTER2, 2L, Counter.COUNTER3, 3L),
            COUNTER_GROUP + "2", ImmutableMap.of(Counter.COUNTER1, 1L, Counter.COUNTER2, 2L)));
    assertCounterLessThan(
        ImmutableMap.of(COUNTER_GROUP + "1", ImmutableMap.of(Counter.COUNTER1, 1L, Counter.COUNTER2, 2L, Counter.COUNTER3, 3L),
            COUNTER_GROUP + "2", ImmutableMap.of(Counter.COUNTER1, 1L, Counter.COUNTER2, 2L)),
        ImmutableMap.of(COUNTER_GROUP + "1", ImmutableMap.of(Counter.COUNTER1, 0L, Counter.COUNTER2, 1L, Counter.COUNTER3, 2L),
            COUNTER_GROUP + "2", ImmutableMap.of(Counter.COUNTER1, 0L, Counter.COUNTER2, 1L)));
    assertCounterGreaterThan(
        ImmutableMap.of(COUNTER_GROUP + "1", ImmutableMap.of(Counter.COUNTER1, 1L, Counter.COUNTER2, 2L, Counter.COUNTER3, 3L),
            COUNTER_GROUP + "2", ImmutableMap.of(Counter.COUNTER1, 1L, Counter.COUNTER2, 2L)),
        ImmutableMap.of(COUNTER_GROUP + "1", ImmutableMap.of(Counter.COUNTER1, 2L, Counter.COUNTER2, 3L, Counter.COUNTER3, 4L),
            COUNTER_GROUP + "2", ImmutableMap.of(Counter.COUNTER1, 2L, Counter.COUNTER2, 3L)));
    assertCounterEqualsLessThanGreaterThan(ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, 1L)), Collections.EMPTY_MAP,
        Collections.EMPTY_MAP, ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, 1L)));
    assertCounterEqualsLessThanGreaterThan(Collections.EMPTY_MAP, ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, 2L)),
        Collections.EMPTY_MAP, ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, 1L)));
    assertCounterEqualsLessThanGreaterThan(Collections.EMPTY_MAP, Collections.EMPTY_MAP,
        ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, 0L)),
        ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, 1L)));
    assertCounterEqualsLessThanGreaterThan(ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, 1L)),
        ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, 2L)),
        ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, 0L)),
        ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, 1L)));
  }

  public static final TestMetaData testMetaData1 = TestMetaData.getInstance() //
      .asserts(ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, 0L)));
  public static final TestMetaData testMetaData2 = TestMetaData.getInstance() //
      .parameters(Collections.EMPTY_MAP) //
      .asserts(ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, 0L)));
  public static final TestMetaData testMetaData3 = TestMetaData.getInstance() //
      .dataSetSourceDirs(DIR_SOURCE) //
      .dataSetDestinationDirs(DIR_DESTINATION) //
      .asserts(ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, 4L)));
  public static final TestMetaData testMetaData4 = TestMetaData.getInstance() //
      .dataSetSourceDirs(DIR_SOURCE) //
      .dataSetNames("dataset-1") //
      .dataSetDestinationDirs(DIR_DESTINATION) //
      .asserts(ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, 2L)));
  public static final TestMetaData testMetaData5 = TestMetaData.getInstance() //
      .dataSetSourceDirs(DIR_SOURCE) //
      .dataSetNames("dataset-1") //
      .dataSetSubsets(new String[][] { { "dataset-1-sub-1" } }) //
      .dataSetDestinationDirs(DIR_DESTINATION) //
      .asserts(ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, 2L)));
  public static final TestMetaData testMetaData6 = TestMetaData.getInstance() //
      .dataSetSourceDirs(DIR_SOURCE) //
      .dataSetNames("dataset-1") //
      .dataSetSubsets(new String[][] { { "dataset-1-sub-1" } }) //
      .dataSetLabels(new String[][][] { { { "dataset-1-sub-1-sub-1" }, } }) //
      .dataSetDestinationDirs(DIR_DESTINATION) //
      .asserts(ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, 2L)));
  public static final TestMetaData testMetaData7 = TestMetaData.getInstance() //
      .dataSetSourceDirs(DIR_SOURCE, DIR_SOURCE) //
      .dataSetNames("dataset-1", "dataset-1") //
      .dataSetSubsets(new String[][] { { "dataset-1-sub-1" }, { "dataset-1-sub-1" } }) //
      .dataSetLabels(new String[][][] { { { "dataset-1-sub-1-sub-1" } }, { { "dataset-1-sub-1-sub-1" } } }) //
      .dataSetDestinationDirs(DIR_DESTINATION + "/one", DIR_DESTINATION + "/two") //
      .asserts(ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, 4L)));

  public void testDfs(TestMetaData testMetaData) throws Exception {
    assertNotNull(testMetaData);
    assertCounterEquals(testMetaData.getAsserts()[0],
        ImmutableMap.of(COUNTER_GROUP, ImmutableMap.of(Counter.COUNTER1, (long) getDfsServer().listFilesDfs("/").length
            - (MrServer.getInstance().isStarted() && MrServer.getInstance().getRuntime().equals(MrServer.Runtime.CLUSTER_JOB) ? 2 : 0))));
  }

  private void assertCopyFromLocalDir(int countUpstream, int countDownstream, String sourcePath, String destinationPath,
      String... sourceLabels) throws Exception {
    assertEquals(countUpstream, getDfsServer().listFilesDfs(destinationPath).length);
    assertTrue(getDfsServer().copyFromLocalDir(sourcePath, destinationPath, sourceLabels).length > 0);
    assertEquals(countDownstream, getDfsServer().listFilesDfs(destinationPath).length);
  }

}
