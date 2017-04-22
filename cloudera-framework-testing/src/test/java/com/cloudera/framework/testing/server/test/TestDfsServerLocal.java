package com.cloudera.framework.testing.server.test;

import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.googlecode.zohhak.api.Coercion;
import com.googlecode.zohhak.api.TestWith;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

@RunWith(TestRunner.class)
public class TestDfsServerLocal extends TestDfsServer {

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance(DfsServer.Runtime.LOCAL_FS);

  @Override
  public DfsServer getDfsServer() {
    return dfsServer;
  }

  @Override
  @TestWith({"testMetaData1", "testMetaData2", "testMetaData3", //
    "testMetaData4", "testMetaData5", "testMetaData6", "testMetaData7"})
  public void testDfs(TestMetaData testMetaData) throws Exception {
    super.testDfs(testMetaData);
  }

  @TestWith({"testMetaData1", "testMetaData2", "testMetaData3", //
    "testMetaData4", "testMetaData5", "testMetaData6", "testMetaData7"})
  public void testDfsAgain(TestMetaData testMetaData) throws Exception {
    super.testDfs(testMetaData);
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
