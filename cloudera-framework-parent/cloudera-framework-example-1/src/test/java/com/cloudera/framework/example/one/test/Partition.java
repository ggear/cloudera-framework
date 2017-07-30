package com.cloudera.framework.example.one.test;

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
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

/**
 * Test dataset partition
 */
@RunWith(TestRunner.class)
public class Partition extends TestBase {

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static MrServer mrServer = MrServer.getInstance();

  /**
   * Test dataset partition
   */
  @TestWith({"testMetaDataCsvPristine", "testMetaDataXmlPristine", "testMetaDataAll"})
  public void testPartition(TestMetaData testMetaData) throws Exception {
    Assert.assertEquals(Driver.RETURN_SUCCESS, new com.cloudera.framework.example.one.process.Stage(dfsServer.getConf()).runner(
      new String[]{dfsServer.getPath(DIR_ABS_MYDS_RAW_CANONICAL).toString(), dfsServer.getPath(DIR_ABS_MYDS_STAGED).toString()}));
    Driver driver = new com.cloudera.framework.example.one.process.Partition(dfsServer.getConf());
    assertEquals(Driver.RETURN_SUCCESS, driver.runner(new String[]{dfsServer.getPath(DIR_ABS_MYDS_STAGED_CANONICAL).toString(),
      dfsServer.getPath(DIR_ABS_MYDS_PARTITIONED).toString()}));
    assertCounterEquals(testMetaData.getAsserts()[0], driver.getCounters());
    assertEquals(Driver.RETURN_SUCCESS, driver.runner(new String[]{dfsServer.getPath(DIR_ABS_MYDS_STAGED_CANONICAL).toString(),
      dfsServer.getPath(DIR_ABS_MYDS_PARTITIONED).toString()}));
    assertCounterEquals(testMetaData.getAsserts()[1], driver.getCounters());
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}