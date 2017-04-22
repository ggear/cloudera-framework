package com.cloudera.framework.testing.server.test;

import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.MrServer;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

@RunWith(TestRunner.class)
public class TestMrServerLocal extends TestMrServer {

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance(DfsServer.Runtime.LOCAL_FS);

  @ClassRule
  public static final MrServer mrServer = MrServer.getInstance(MrServer.Runtime.LOCAL_JOB);

  @Override
  public DfsServer getDfsServer() {
    return dfsServer;
  }

  @Override
  public MrServer getMrServer() {
    return mrServer;
  }

}
