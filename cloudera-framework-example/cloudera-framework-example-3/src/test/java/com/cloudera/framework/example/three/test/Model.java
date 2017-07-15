package com.cloudera.framework.example.three.test;


import static com.cloudera.framework.example.three.Driver.TestDir;
import static com.cloudera.framework.example.three.Driver.TrainDir;
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
import org.dmg.pmml.PMML;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

/**
 * Test model
 */
@RunWith(TestRunner.class)
public class Model implements TestConstants {

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static final SparkServer sparkServer = SparkServer.getInstance();

  private static final String DATASET = "roomsensors";
  private static final String DATASET_TRAIN = "train";
  private static final String DATASET_TEST = "test";

  public final TestMetaData testMetaDataAll = TestMetaData.getInstance() //
    .dataSetSourceDirs(REL_DIR_DATASET, REL_DIR_DATASET) //
    .dataSetNames(DATASET, DATASET) //
    .dataSetSubsets(new String[][]{{DATASET_TRAIN}, {DATASET_TEST}}) //
    .dataSetLabels(new String[][][]{{{null},}, {{null},}}) //
    .dataSetDestinationDirs("/" + DATASET + "/" + TrainDir(), "/" + DATASET + "/" + TestDir());

  /**
   * Test model
   */
  @TestWith({"testMetaDataAll"})
  public void testModel(TestMetaData testMetaData) throws Exception {
    Driver driver = new com.cloudera.framework.example.three.Driver(dfsServer.getConf());
    assertEquals(Driver.RETURN_SUCCESS, driver.runner(new String[]{dfsServer.getPath(DATASET).toString()}));
    assertTrue(Double.parseDouble(((PMML) driver.getResults().get(0)).getHeader().getExtensions().get(0).getContent().get(0).toString()) > 0.9);
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
