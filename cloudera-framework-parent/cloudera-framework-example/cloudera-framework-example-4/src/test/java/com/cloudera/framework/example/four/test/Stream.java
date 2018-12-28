package com.cloudera.framework.example.four.test;

import static org.junit.Assert.assertEquals;

import java.util.Collections;

import com.cloudera.framework.common.Driver;
import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.EnvelopeServer;
import com.cloudera.framework.testing.server.KafkaServer;
import com.cloudera.framework.testing.server.KuduServer;
import com.cloudera.framework.testing.server.SparkServer;
import com.cloudera.labs.envelope.run.Runner;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.googlecode.zohhak.api.Coercion;
import com.googlecode.zohhak.api.TestWith;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.runner.RunWith;

/**
 * Test process
 */
@Ignore
@RunWith(TestRunner.class)
public class Stream implements TestConstants {

  // TODO: Provide an implementation that leverages Kafka, Spark2 Streaming, Kudu and HDFS

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static final KuduServer kuduServer = KuduServer.getInstance();

  @ClassRule
  public static final KafkaServer kafkaServer = KafkaServer.getInstance();

  @ClassRule
  public static final SparkServer sparkServer = SparkServer.getInstance();

  @ClassRule
  public static final EnvelopeServer envelopeServer = EnvelopeServer.getInstance();

  private static final String DATASET_Name = "mydataset";
  private static final String DATASET_DIR = "/" + DATASET_Name;
  private static final String DATASET_DIR_CSV = DATASET_DIR + "/csv";
  private static final String DATASET_DIR_PARQUET = DATASET_DIR + "/parquet";

  public final TestMetaData testMetaDataAll = TestMetaData.getInstance() //
    .dataSetSourceDirs(REL_DIR_DATASET) //
    .dataSetDestinationDirs(DATASET_DIR_CSV);

  /**
   * Test process
   */
  @TestWith({"testMetaDataAll"})
  public void testStream(TestMetaData testMetaData) throws Exception {
    Driver driver = new com.cloudera.framework.example.four.Stream(dfsServer.getConf());
    assertEquals(Driver.SUCCESS, driver.runner(dfsServer.getPathUri(DATASET_DIR_CSV), dfsServer.getPathUri(DATASET_DIR_PARQUET)));
    SparkSession sparkSession = SparkSession.builder().config(new SparkConf()).getOrCreate();
    Dataset<Row> datasetCsv = sparkSession.read().option("header", true).csv(dfsServer.getPathUri(DATASET_DIR_CSV));
    Dataset<Row> datasetParquet = sparkSession.read().parquet(dfsServer.getPathUri(DATASET_DIR_PARQUET));
    assertEquals(datasetCsv.filter(datasetCsv.col("Date").isNotNull()).count(),
      datasetParquet.filter(datasetParquet.col("Date").isNotNull()).count());
    assertEquals(datasetCsv.filter(datasetCsv.col("String").like("%qfvshkd%")).count(),
      datasetParquet.filter(datasetParquet.col("String").like("%qfvshkd%")).count());
    sparkSession.close();
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
