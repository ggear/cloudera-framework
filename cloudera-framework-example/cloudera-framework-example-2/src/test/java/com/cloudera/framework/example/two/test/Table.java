package com.cloudera.framework.example.two.test;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Arrays;

import com.cloudera.framework.common.util.FsUtil;
import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.HiveServer;
import com.cloudera.framework.testing.server.HiveServer.Runtime;
import com.googlecode.zohhak.api.Coercion;
import com.googlecode.zohhak.api.TestWith;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

/**
 * Test dataset tables
 */
@RunWith(TestRunner.class)
public class Table implements TestConstants {

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static final HiveServer hiveServer = HiveServer.getInstance(Runtime.LOCAL_SPARK);

  private static final String DATASET = "mydataset";
  private static final String DATASET_DATABASE_DESTINATION = "/" + DATASET;
  private static final String DATASET_TABLE_SOURCE = REL_DIR_DATASET;
  private static final String DATASET_TABLE_DESTINATION = DATASET_DATABASE_DESTINATION + "/mytable";

  private static final String DATASET_DDL_VAR_DATABASE = "my.database.name";
  private static final String DATASET_DDL_VAR_LOCATION = "my.table.location";

  public final TestMetaData testMetaDataAll = TestMetaData.getInstance() //
    .dataSetSourceDirs(DATASET_TABLE_SOURCE) //
    .dataSetDestinationDirs(DATASET_TABLE_DESTINATION);

  /**
   * Test dataset tables
   */
  @TestWith({"testMetaDataAll"})
  public void testTable(TestMetaData testMetaData) throws Exception {
    hiveServer.execute("CREATE DATABASE IF NOT EXISTS " + DATASET + " LOCATION '" + dfsServer.getPathUri(DATASET_DATABASE_DESTINATION) + "'");
    hiveServer.execute("SHOW DATABASES");
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

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
