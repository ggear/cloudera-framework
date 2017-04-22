package com.cloudera.framework.testing.server.test;

import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.SparkServer;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

@RunWith(TestRunner.class)
@SuppressWarnings("serial")
public class TestSparkServerLocal extends TestSparkServer {

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance(DfsServer.Runtime.LOCAL_FS);

  @ClassRule
  public static final SparkServer sparkServer = SparkServer.getInstance();

  @Override
  public DfsServer getDfsServer() {
    return dfsServer;
  }

  @Override
  public SparkServer getSparkServer() {
    return sparkServer;
  }

}
