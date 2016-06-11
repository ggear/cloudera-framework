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
 * Test dataset process
 */
@RunWith(TestRunner.class)
public class Process extends TestBase {

  @ClassRule
  public static DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static MrServer mrServer = MrServer.getInstance();

  /**
   * Test dataset process
   */
  @TestWith({ "testMetaDataCsvPristine", "testMetaDataXmlPristine", "testMetaDataAll" })
  public void testProcess(TestMetaData testMetaData) throws Exception {
    assertEquals(Driver.RETURN_SUCCESS, new com.cloudera.example.ingest.Stage(dfsServer.getConf()).runner(new String[] {
        dfsServer.getPath(DIR_ABS_MYDS_RAW_CANONICAL).toString(), dfsServer.getPath(DIR_ABS_MYDS_STAGED).toString() }));
    assertEquals(Driver.RETURN_SUCCESS,
        new com.cloudera.example.ingest.Partition(dfsServer.getConf())
            .runner(new String[] { dfsServer.getPath(DIR_ABS_MYDS_STAGED_CANONICAL).toString(),
                dfsServer.getPath(DIR_ABS_MYDS_PARTITIONED).toString() }));
    Driver driver = new com.cloudera.example.ingest.Process(dfsServer.getConf());
    assertEquals(Driver.RETURN_SUCCESS,
        driver.runner(new String[] { dfsServer.getPath(DIR_ABS_MYDS_PARTITIONED_CANONICAL).toString(),
            dfsServer.getPath(DIR_ABS_MYDS_PROCESSED).toString() }));
    assertCounterEquals(testMetaData.getAsserts()[0], driver.getCounters());
    assertEquals(Driver.RETURN_SUCCESS,
        driver.runner(new String[] { dfsServer.getPath(DIR_ABS_MYDS_PARTITIONED_CANONICAL).toString(),
            dfsServer.getPath(DIR_ABS_MYDS_PROCESSED).toString() }));
    assertCounterEquals(testMetaData.getAsserts()[1], driver.getCounters());
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
