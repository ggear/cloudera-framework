package com.cloudera.framework.testing.server.tests;

import org.junit.ClassRule;
import org.junit.runner.RunWith;

import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.googlecode.zohhak.api.Coercion;
import com.googlecode.zohhak.api.TestWith;

@RunWith(TestRunner.class)
public class TestDfsServerCluster extends TestDfsServer {

  @ClassRule
  public static DfsServer dfsServer = DfsServer.getInstance(DfsServer.Runtime.CLUSTER_DFS);

  @Override
  public DfsServer getDfsServer() {
    return dfsServer;
  }

  @Override
  @TestWith({ "testMetaData1", "testMetaData2", "testMetaData3", //
      "testMetaData4", "testMetaData5", "testMetaData6", "testMetaData7" })
  public void testCdhMetaData(TestMetaData testMetaData) throws Exception {
    super.testCdhMetaData(testMetaData);
  }

  @TestWith({ "testMetaData1", "testMetaData2", "testMetaData3", //
      "testMetaData4", "testMetaData5", "testMetaData6", "testMetaData7" })
  public void testCdhMetaDataAgain(TestMetaData testMetaData) throws Exception {
    super.testCdhMetaData(testMetaData);
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
