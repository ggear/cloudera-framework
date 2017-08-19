package com.cloudera.framework.example.one.test;

import static com.cloudera.framework.common.Driver.SUCCESS;
import static com.cloudera.framework.testing.Assert.assertCounterEquals;
import static org.junit.Assert.assertEquals;

import com.cloudera.framework.common.Driver;
import com.cloudera.framework.example.one.TestBase;
import com.cloudera.framework.example.one.process.Stage;
import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.MrServer;
import com.googlecode.zohhak.api.Coercion;
import com.googlecode.zohhak.api.TestWith;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

/**
 * Test dataset cleanse
 */
@RunWith(TestRunner.class)
public class Cleanse extends TestBase {

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static MrServer mrServer = MrServer.getInstance();

  /**
   * Test dataset cleanse
   */
  @TestWith({"testMetaDataCsvPristine", "testMetaDataXmlPristine", "testMetaDataAll"})
  public void testCleanse(TestMetaData testMetaData) throws Exception {
    assertEquals(SUCCESS, new Stage(dfsServer.getConf()).runner(
      dfsServer.getPath(DIR_ABS_MYDS_RAW_CANONICAL).toString(), dfsServer.getPath(DIR_ABS_MYDS_STAGED).toString()));
    assertEquals(SUCCESS, new com.cloudera.framework.example.one.process.Partition(dfsServer.getConf()).runner(dfsServer.getPath
      (DIR_ABS_MYDS_STAGED_CANONICAL).toString(), dfsServer.getPath(DIR_ABS_MYDS_PARTITIONED).toString()));
    Driver driver = new com.cloudera.framework.example.one.process.Cleanse(dfsServer.getConf());
    assertEquals(SUCCESS, driver.runner(dfsServer.getPath(DIR_ABS_MYDS_PARTITIONED_CANONICAL).toString(),
      dfsServer.getPath(DIR_ABS_MYDS_CLEANSED).toString()));
    assertCounterEquals(testMetaData, driver.getCounters());
    assertEquals(SUCCESS, driver.runner(dfsServer.getPath(DIR_ABS_MYDS_PARTITIONED_CANONICAL).toString(),
      dfsServer.getPath(DIR_ABS_MYDS_CLEANSED).toString()));
    assertCounterEquals(testMetaData, 1, driver.getCounters());
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
