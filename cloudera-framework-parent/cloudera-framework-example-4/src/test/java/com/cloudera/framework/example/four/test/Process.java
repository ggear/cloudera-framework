package com.cloudera.framework.example.four.test;

import static org.junit.Assert.assertEquals;

import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.KafkaServer;
import com.cloudera.framework.testing.server.KuduServer;
import com.cloudera.framework.testing.server.SparkServer;
import com.googlecode.zohhak.api.Coercion;
import com.googlecode.zohhak.api.TestWith;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

/**
 * Test process
 */
@RunWith(TestRunner.class)
public class Process implements TestConstants {

  // TODO: Extract to a Driver and provide an implementation that leverages Kafka, Spark2 Streaming, Kudu and HDFS

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static final KuduServer kuduServer = KuduServer.getInstance();

  @ClassRule
  public static final KafkaServer kafkaServer = KafkaServer.getInstance();

  @ClassRule
  public static final SparkServer sparkServer = SparkServer.getInstance();

  private static final String DATASET = "mydataset";
  private static final String DATASET_DIR = "/" + DATASET;
  private static final String DATASET_INPUT_DIR = DATASET_DIR + "/mytable";

  public final TestMetaData testMetaDataAll = TestMetaData.getInstance() //
    .dataSetSourceDirs(REL_DIR_DATASET) //
    .dataSetDestinationDirs(DATASET_INPUT_DIR);

  /**
   * Test process
   */
  @TestWith({"testMetaDataAll"})
  public void testProcess(TestMetaData testMetaData) throws Exception {
    SparkContext sparkContext = new SparkContext(new SparkConf());
    Dataset<Row> dataset = new SparkSession(sparkContext).read().format("com.databricks.spark.csv").load(dfsServer.getPathUri
      (DATASET_INPUT_DIR));
    assertEquals(4, dataset.filter(dataset.col("_c0").isNotNull()).count());
    assertEquals(1, dataset.filter(dataset.col("_c2").like("%0.1293083612314587%")).count());
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
