package com.cloudera.framework.example.three.test;


import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.KuduServer;
import com.cloudera.framework.testing.server.SparkServer;
import com.googlecode.zohhak.api.Coercion;
import com.googlecode.zohhak.api.TestWith;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

/**
 * Test process
 */
@RunWith(TestRunner.class)
public class Process implements TestConstants {

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static final KuduServer kuduServer = KuduServer.getInstance();

  @ClassRule
  public static final SparkServer sparkServer = SparkServer.getInstance();

  private static final String DATASET = "mydataset";
  private static final String DATASET_FIELDS = "myfields";
  private static final String DATASET_DATABASE_DESTINATION = "/" + DATASET;
  private static final String DATASET_TABLE_SOURCE = REL_DIR_DATASET;
  private static final String DATASET_TABLE_DESTINATION = DATASET_DATABASE_DESTINATION + "/mytable";
  private static final String DATASET_TABLE_DESTINATION_FILE = DATASET_TABLE_DESTINATION + "/mydataset_pristine.csv";

  public final TestMetaData testMetaDataAll = TestMetaData.getInstance() //
    .dataSetSourceDirs(DATASET_TABLE_SOURCE) //
    .dataSetDestinationDirs(DATASET_TABLE_DESTINATION);

  /**
   * Test process
   */
  @TestWith({"testMetaDataAll"})
  public void testProcess(TestMetaData testMetaData) throws Exception {

    // TODO: Provide an implementation that leverages Spark2, Kudu and HDFS

    Dataset dataset = new SQLContext(SparkServer.getInstance().getContext()).createDataFrame(
      SparkServer.getInstance().getContext().textFile(DfsServer.getInstance().getPathUri(DATASET_TABLE_DESTINATION_FILE)).map(RowFactory::create),
      DataTypes.createStructType(Arrays.asList(DataTypes.createStructField(DATASET_FIELDS, DataTypes.StringType, true))));
    assertEquals(4, dataset.filter(dataset.col(DATASET_FIELDS).isNotNull()).count());
    assertEquals(1, dataset.filter(dataset.col(DATASET_FIELDS).like("%0.1293083612314587%")).count());
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
