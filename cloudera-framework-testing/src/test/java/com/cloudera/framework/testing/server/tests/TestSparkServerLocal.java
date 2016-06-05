package com.cloudera.framework.testing.server.tests;

import org.junit.ClassRule;
import org.junit.runner.RunWith;

import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.SparkServer;

@RunWith(TestRunner.class)
@SuppressWarnings("serial")
public class TestSparkServerLocal extends TestSparkServer {

  @ClassRule
  public static DfsServer dfsServer = DfsServer.getInstance(DfsServer.Runtime.LOCAL_FS);

  @ClassRule
  public static SparkServer sparkServer = SparkServer.getInstance();

  @Override
  public DfsServer getDfsServer() {
    return dfsServer;
  }

  @Override
  public SparkServer getSparkServer() {
    return sparkServer;
  }

}
