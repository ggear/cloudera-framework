package com.cloudera.framework.example.test;

import static com.cloudera.framework.testing.Assert.assertCounterEquals;
import static org.junit.Assert.assertEquals;

import com.cloudera.framework.common.Driver;
import com.cloudera.framework.example.TestBase;
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
  public static DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static MrServer mrServer = MrServer.getInstance();

  /**
   * Test dataset cleanse
   */
  @TestWith({"testMetaDataCsvPristine", "testMetaDataXmlPristine", "testMetaDataAll"})
  public void testCleanse(TestMetaData testMetaData) throws Exception {
    assertEquals(Driver.RETURN_SUCCESS, new com.cloudera.framework.example.process.Stage(dfsServer.getConf()).runner(
      new String[]{dfsServer.getPath(DIR_ABS_MYDS_RAW_CANONICAL).toString(), dfsServer.getPath(DIR_ABS_MYDS_STAGED).toString()}));
    assertEquals(Driver.RETURN_SUCCESS, new com.cloudera.framework.example.process.Partition(dfsServer.getConf()).runner(new String[]{
      dfsServer.getPath(DIR_ABS_MYDS_STAGED_CANONICAL).toString(), dfsServer.getPath(DIR_ABS_MYDS_PARTITIONED).toString()}));
    Driver driver = new com.cloudera.framework.example.process.Cleanse(dfsServer.getConf());
    assertEquals(Driver.RETURN_SUCCESS, driver.runner(new String[]{dfsServer.getPath(DIR_ABS_MYDS_PARTITIONED_CANONICAL).toString(),
      dfsServer.getPath(DIR_ABS_MYDS_CLEANSED).toString()}));
    assertCounterEquals(testMetaData.getAsserts()[0], driver.getCounters());
    assertEquals(Driver.RETURN_SUCCESS, driver.runner(new String[]{dfsServer.getPath(DIR_ABS_MYDS_PARTITIONED_CANONICAL).toString(),
      dfsServer.getPath(DIR_ABS_MYDS_CLEANSED).toString()}));
    assertCounterEquals(testMetaData.getAsserts()[1], driver.getCounters());
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}