package com.cloudera.framework.example.five.test;

import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.SparkServer;
import com.googlecode.zohhak.api.Coercion;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

/**
 * Test process
 */
@RunWith(TestRunner.class)
public class Process implements TestConstants {

  // TODO: Extract to a Driver and provide an implementation that leverages Python, Numpy/Pandas, PySpark, Spark2, HDFS

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static final SparkServer sparkServer = SparkServer.getInstance();

  private static final String DATASET = "mydataset";
  private static final String DATASET_DIR = "/" + DATASET;
  private static final String DATASET_INPUT_DIR = DATASET_DIR + "/mytable";

  public final TestMetaData testMetaDataAll = TestMetaData.getInstance() //
    .dataSetSourceDirs(REL_DIR_DATASET) //
    .dataSetDestinationDirs(DATASET_INPUT_DIR);

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
