package com.cloudera.example.test;

import static com.cloudera.framework.testing.Assert.assertCounterEquals;
import static org.junit.Assert.assertEquals;

import org.junit.ClassRule;
import org.junit.runner.RunWith;

import com.cloudera.example.TestBase;
import com.cloudera.framework.common.Driver;
import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.MrServer;
import com.googlecode.zohhak.api.Coercion;
import com.googlecode.zohhak.api.TestWith;

/**
 * Test dataset stage
 */
@RunWith(TestRunner.class)
public class Stage extends TestBase {

  @ClassRule
  public static DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static MrServer mrServer = MrServer.getInstance();

  /**
   * Test stage
   */
  @TestWith({ "testMetaDataCsvPristine", "testMetaDataXmlPristine", "testMetaDataAll" })
  public void testStage(TestMetaData testMetaData) throws Exception {
    Driver driver = new com.cloudera.example.ingest.Stage(dfsServer.getConf());
    assertEquals(Driver.RETURN_SUCCESS, driver.runner(new String[] {
        dfsServer.getPath(DIR_ABS_MYDS_RAW_CANONICAL).toString(), dfsServer.getPath(DIR_ABS_MYDS_STAGED).toString() }));
    assertCounterEquals(testMetaData.getAsserts()[0], driver.getCounters());
    assertEquals(Driver.RETURN_SUCCESS, driver.runner(new String[] {
        dfsServer.getPath(DIR_ABS_MYDS_RAW_CANONICAL).toString(), dfsServer.getPath(DIR_ABS_MYDS_STAGED).toString() }));
    assertCounterEquals(testMetaData.getAsserts()[1], driver.getCounters());
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
