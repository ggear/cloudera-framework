package com.cloudera.framework.example.two.test;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;

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
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Before;
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
  public static final HiveServer hiveServer = HiveServer.getInstance(Runtime.LOCAL_SPARK);

  @ClassRule
  public static final SparkServer sparkServer = SparkServer.getInstance();

  private static final String DATASET = "mydataset";
  private static final String DATASET_FIELDS = "myfields";
  private static final String DATASET_DATABASE_DESTINATION = "/" + DATASET;
  private static final String DATASET_TABLE_SOURCE = REL_DIR_DATASET;
  private static final String DATASET_TABLE_DESTINATION = DATASET_DATABASE_DESTINATION + "/mytable";
  private static final String DATASET_TABLE_DESTINATION_FILE = DATASET_TABLE_DESTINATION + "/mydataset_pristine.csv";

  private static final String DATASET_DDL_VAR_DATABASE = "my.database.name";
  private static final String DATASET_DDL_VAR_LOCATION = "my.table.location";

  public final TestMetaData testMetaDataAll = TestMetaData.getInstance() //
    .dataSetSourceDirs(DATASET_TABLE_SOURCE) //
    .dataSetDestinationDirs(DATASET_TABLE_DESTINATION);

  @Before
  public void setupDatabase() throws Exception {
    hiveServer.execute("CREATE DATABASE IF NOT EXISTS " + DATASET + " LOCATION '" + dfsServer.getPathUri(DATASET_DATABASE_DESTINATION) + "'");
  }

  /**
   * Test process
   */
  @TestWith({"testMetaDataAll"})
  public void testProcess(TestMetaData testMetaData) throws Exception {

    // TODO: Provide an implementation that leverages Impala and S3

    executeSpark();
    executeHive();
    executeSpark();
    executeSpark();
    executeHive();
  }

  private void executeHive() throws Exception {
    for (File script : FsUtil.listFiles(ABS_DIR_HIVE_SCHEMA)) {
      assertEquals(Arrays.asList(0, 0), HiveServer.count(hiveServer.execute(script)));
    }
    for (File script : FsUtil.listFiles(ABS_DIR_HIVE_REFRESH)) {
      assertEquals(Arrays.asList(0, 0), HiveServer.count(hiveServer.execute(script)));
    }
    for (File script : FsUtil.listFiles(ABS_DIR_HIVE_QUERY)) {
      assertEquals(Arrays.asList(0, 10, 7, 3), HiveServer.count(hiveServer.execute(script)));
    }
  }

  private void executeSpark() {
    JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf());
    DataFrame dataFrame = new SQLContext(sparkContext).createDataFrame(
      sparkContext.textFile(dfsServer.getPathUri(DATASET_TABLE_DESTINATION_FILE)).map(RowFactory::create),
      DataTypes.createStructType(Collections.singletonList(DataTypes.createStructField(DATASET_FIELDS, DataTypes.StringType, true))));
    assertEquals(4, dataFrame.filter(dataFrame.col(DATASET_FIELDS).isNotNull()).count());
    assertEquals(1, dataFrame.filter(dataFrame.col(DATASET_FIELDS).like("%0.1293083612314587%")).count());
    sparkContext.close();
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
