package com.cloudera.framework.testing.server.test;

import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.HiveServer;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

@RunWith(TestRunner.class)
public class TestHiveServerSparkCluster extends TestHiveServer {

  @ClassRule
  public static final HiveServer hiveServer = HiveServer.getInstance(HiveServer.Runtime.LOCAL_SPARK);

  @Override
  public HiveServer getHiveServer() {
    return hiveServer;
  }

}
