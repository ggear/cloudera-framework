package com.cloudera.framework.testing.server.test;

import org.junit.ClassRule;
import org.junit.runner.RunWith;

import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.HiveServer;

@RunWith(TestRunner.class)
public class TestHiveServerMrCluster extends TestHiveServer {

  @ClassRule
  public static HiveServer hiveServer = HiveServer.getInstance(HiveServer.Runtime.CLUSTER_MR2);

  @Override
  public HiveServer getHiveServer() {
    return hiveServer;
  }

}
