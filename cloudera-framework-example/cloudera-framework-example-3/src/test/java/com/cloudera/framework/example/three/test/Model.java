package com.cloudera.framework.example.three.test;


import static org.junit.Assert.assertEquals;

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

  private static final String DATASET_DIR = "/roomsensors";

  public final TestMetaData testMetaDataAll = TestMetaData.getInstance() //
    .dataSetSourceDirs(REL_DIR_DATASET) //
    .dataSetDestinationDirs(DATASET_DIR + "/" + com.cloudera.framework.example.three.Driver.RawLabel());

  /**
   * Test model
   */
  @TestWith({"testMetaDataAll"})
  public void testModel(TestMetaData testMetaData) throws Exception {
    Driver driver = new com.cloudera.framework.example.three.Driver(dfsServer.getConf());
    assertEquals(Driver.RETURN_SUCCESS, driver.runner(new String[]{dfsServer.getPath(DATASET_DIR).toString()}));
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
