package com.cloudera.framework.example.two.test;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.cloudera.framework.common.Driver;
import com.cloudera.framework.common.util.FsUtil;
import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.HiveServer;
import com.cloudera.framework.testing.server.HiveServer.Runtime;
import com.cloudera.framework.testing.server.SparkServer;
import com.googlecode.zohhak.api.Coercion;
import com.googlecode.zohhak.api.TestWith;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

/**
 * Test process
 */
@RunWith(TestRunner.class)
public class Process implements TestConstants {

  // TODO: Provide an implementation that leverages Impala and S3

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static final HiveServer hiveServer = HiveServer.getInstance(Runtime.LOCAL_SPARK);

  @ClassRule
  public static final SparkServer sparkServer = SparkServer.getInstance();

  private static final String DATASET = "mydataset";
  private static final String DATASET_DIR = "/" + DATASET;
  private static final String DATASET_INPUT_DIR = DATASET_DIR + "/mytable";

  public final TestMetaData testMetaDataAll = TestMetaData.getInstance() //
    .dataSetSourceDirs(REL_DIR_DATASET) //
    .dataSetDestinationDirs(DATASET_INPUT_DIR);

  @Before
  public void setUp() throws Exception {
    hiveServer.execute("CREATE DATABASE IF NOT EXISTS " + DATASET +
      " LOCATION '" + dfsServer.getPathUri(DATASET_DIR) + "'");
  }

  /**
   * Test process
   */
  @TestWith({"testMetaDataAll"})
  public void testProcessSingle(TestMetaData testMetaData) throws Exception {
    assertEquals(executeHive(), executeSpark());
  }

  /**
   * Test process
   */
  @TestWith({"testMetaDataAll"})
  public void testProcessInterleaved(TestMetaData testMetaData) throws Exception {
    assertEquals(executeSpark(), executeHive());
    assertEquals(executeHive(), executeSpark());
  }

  private List<String> executeSpark() {
    Driver driver = new com.cloudera.framework.example.two.process.Process(dfsServer.getConf());
    assertEquals(Driver.RETURN_SUCCESS, driver.runner(new String[]{dfsServer.getPath(DATASET_INPUT_DIR).toString()}));
    return driver.getResults();
  }

  private List<String> executeHive() throws Exception {
    List<List<String>> results = Collections.emptyList();
    for (File script : FsUtil.listFiles(ABS_DIR_HIVE_SCHEMA)) {
      assertEquals(Arrays.asList(0, 0), HiveServer.count(hiveServer.execute(script)));
    }
    for (File script : FsUtil.listFiles(ABS_DIR_HIVE_REFRESH)) {
      assertEquals(Arrays.asList(0, 0), HiveServer.count(hiveServer.execute(script)));
    }
    for (File script : FsUtil.listFiles(ABS_DIR_HIVE_QUERY)) {
      results = hiveServer.execute(script);
    }
    return results.size() > 0 ? results.get(results.size() - 1) : Collections.emptyList();
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
