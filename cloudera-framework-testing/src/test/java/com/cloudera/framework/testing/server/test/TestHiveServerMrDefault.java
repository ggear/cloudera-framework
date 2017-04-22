package com.cloudera.framework.testing.server.test;

import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.HiveServer;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

@RunWith(TestRunner.class)
public class TestHiveServerMrDefault extends TestHiveServer {

  @ClassRule
  public static final HiveServer hiveServer = HiveServer.getInstance();

  @Override
  public HiveServer getHiveServer() {
    return hiveServer;
  }

}
