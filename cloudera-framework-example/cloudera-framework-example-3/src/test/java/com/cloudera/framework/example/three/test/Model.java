package com.cloudera.framework.example.three.test;


import static com.cloudera.framework.example.three.Driver.ModelLabel;
import static com.cloudera.framework.example.three.Driver.RawLabel;
import static com.cloudera.framework.example.three.Driver.TestLabel;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.cloudera.framework.common.Driver;
import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.SparkServer;
import com.googlecode.zohhak.api.Coercion;
import com.googlecode.zohhak.api.TestWith;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

/**
 * Test model
 */
@RunWith(TestRunner.class)
public class Model implements TestConstants {

  // TODO: Extract to a Driver and provide an implementation that leverages Spark2, MLlib, PMML and HDFS

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static final SparkServer sparkServer = SparkServer.getInstance();

  private static final String DATASET = "roomsensors";
  private static final String DATASET_RAW = "raw";
  private static final String DATASET_TEST = "test";

  public final TestMetaData testMetaDataAll = TestMetaData.getInstance() //
    .dataSetSourceDirs(REL_DIR_DATASET, REL_DIR_DATASET) //
    .dataSetNames(DATASET, DATASET) //
    .dataSetSubsets(new String[][]{{DATASET_RAW}, {DATASET_TEST}}) //
    .dataSetLabels(new String[][][]{{{null},}, {{null},}}) //
    .dataSetDestinationDirs("/" + DATASET + "/" + RawLabel(), "/" + DATASET + "/" + TestLabel());

  /**
   * Test model
   */
  @TestWith({"testMetaDataAll"})
  public void testModel(TestMetaData testMetaData) throws Exception {
    Driver driver = new com.cloudera.framework.example.three.Driver(dfsServer.getConf());
    assertEquals(Driver.RETURN_SUCCESS, driver.runner(new String[]{dfsServer.getPath(DATASET).toString()}));
    assertTrue(0.9 < com.cloudera.framework.example.three.Model.accuracy(dfsServer.getConf(),
      dfsServer.getPath("/" + DATASET + "/" + TestLabel()).toString(),
      dfsServer.getPath("/" + DATASET + "/" + ModelLabel()).toString()));
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
