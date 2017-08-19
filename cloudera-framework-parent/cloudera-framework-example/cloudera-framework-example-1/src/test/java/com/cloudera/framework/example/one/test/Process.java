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
 * Test dataset process
 */
@RunWith(TestRunner.class)
public class Process extends TestBase {

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static MrServer mrServer = MrServer.getInstance();

  /**
   * Test dataset process
   */
  @TestWith({"testMetaDataCsvPristine", "testMetaDataXmlPristine", "testMetaDataAll"})
  public void testProcess(TestMetaData testMetaData) throws Exception {
    Driver driver = new com.cloudera.framework.example.one.process.Process(dfsServer.getConf());
    assertEquals(SUCCESS,
      driver.runner(dfsServer.getPath(DIR_ABS_MYDS_RAW).toString(), dfsServer.getPath(DIR_ABS_MYDS_STAGED).toString(),
        dfsServer.getPath(DIR_ABS_MYDS_PARTITIONED).toString(), dfsServer.getPath(DIR_ABS_MYDS_CLEANSED).toString()));
    assertCounterEquals(testMetaData, driver.getCounters());
    assertEquals(SUCCESS,
      driver.runner(dfsServer.getPath(DIR_ABS_MYDS_RAW).toString(), dfsServer.getPath(DIR_ABS_MYDS_STAGED).toString(),
        dfsServer.getPath(DIR_ABS_MYDS_PARTITIONED).toString(), dfsServer.getPath(DIR_ABS_MYDS_CLEANSED).toString()));
    assertCounterEquals(testMetaData, 1, driver.getCounters());
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
