package com.cloudera.framework.testing.server.tests;

import org.junit.ClassRule;
import org.junit.runner.RunWith;

import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.HiveServer;

@RunWith(TestRunner.class)
public class TestHiveServerMrDefault extends TestHiveServer {

  @ClassRule
  public static HiveServer hiveServer = HiveServer.getInstance();

  @Override
  public HiveServer getHiveServer() {
    return hiveServer;
  }

}
