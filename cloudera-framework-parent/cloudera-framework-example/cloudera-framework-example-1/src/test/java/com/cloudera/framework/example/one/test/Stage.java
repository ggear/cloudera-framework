package com.cloudera.framework.example.one.test;

import static com.cloudera.framework.common.Driver.SUCCESS;
import static com.cloudera.framework.testing.Assert.assertCounterEquals;
import static org.junit.Assert.assertEquals;

import com.cloudera.framework.common.Driver;
import com.cloudera.framework.example.one.TestBase;
import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.MrServer;
import com.googlecode.zohhak.api.Coercion;
import com.googlecode.zohhak.api.TestWith;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

/**
 * Test dataset stage
 */
@RunWith(TestRunner.class)
public class Stage extends TestBase {

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static MrServer mrServer = MrServer.getInstance();

  /**
   * Test stage
   */
  @TestWith({"testMetaDataCsvPristine", "testMetaDataXmlPristine", "testMetaDataAll"})
  public void testStage(TestMetaData testMetaData) throws Exception {
    Driver driver = new com.cloudera.framework.example.one.process.Stage(dfsServer.getConf());
    assertEquals(SUCCESS, driver.runner(
      dfsServer.getPath(DIR_ABS_MYDS_RAW_CANONICAL).toString(), dfsServer.getPath(DIR_ABS_MYDS_STAGED).toString()));
    assertCounterEquals(testMetaData, driver.getCounters());
    assertEquals(SUCCESS, driver.runner(
      dfsServer.getPath(DIR_ABS_MYDS_RAW_CANONICAL).toString(), dfsServer.getPath(DIR_ABS_MYDS_STAGED).toString()));
    assertCounterEquals(testMetaData, 1, driver.getCounters());
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
