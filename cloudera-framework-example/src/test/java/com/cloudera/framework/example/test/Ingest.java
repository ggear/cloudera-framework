package com.cloudera.framework.example.test;

import static com.cloudera.framework.testing.Assert.assertCounterEquals;
import static org.junit.Assert.assertEquals;

import org.junit.ClassRule;
import org.junit.runner.RunWith;

import com.cloudera.framework.common.Driver;
import com.cloudera.framework.example.TestBase;
import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.MrServer;
import com.googlecode.zohhak.api.Coercion;
import com.googlecode.zohhak.api.TestWith;

/**
 * Test dataset ingest
 */
@RunWith(TestRunner.class)
public class Ingest extends TestBase {

  @ClassRule
  public static DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static MrServer mrServer = MrServer.getInstance();

  /**
   * Test dataset ingest
   */
  @TestWith({ "testMetaDataCsvPristine", "testMetaDataXmlPristine", "testMetaDataAll" })
  public void testIngest(TestMetaData testMetaData) throws Exception {
    Driver driver = new com.cloudera.framework.example.ingest.Ingest(dfsServer.getConf());
    assertEquals(Driver.RETURN_SUCCESS,
        driver.runner(new String[] { dfsServer.getPath(DIR_ABS_MYDS_RAW).toString(),
            dfsServer.getPath(DIR_ABS_MYDS_STAGED).toString(), dfsServer.getPath(DIR_ABS_MYDS_PARTITIONED).toString(),
            dfsServer.getPath(DIR_ABS_MYDS_PROCESSED).toString() }));
    assertCounterEquals(testMetaData.getAsserts()[0], driver.getCounters());
    assertEquals(Driver.RETURN_SUCCESS,
        driver.runner(new String[] { dfsServer.getPath(DIR_ABS_MYDS_RAW).toString(),
            dfsServer.getPath(DIR_ABS_MYDS_STAGED).toString(), dfsServer.getPath(DIR_ABS_MYDS_PARTITIONED).toString(),
            dfsServer.getPath(DIR_ABS_MYDS_PROCESSED).toString() }));
    assertCounterEquals(testMetaData.getAsserts()[1], driver.getCounters());
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
