package com.cloudera.framework.testing.server.test;

import org.junit.ClassRule;
import org.junit.runner.RunWith;

import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.MrServer;

@RunWith(TestRunner.class)
public class TestMrServerCluster extends TestMrServer {

  @ClassRule
  public static DfsServer dfsServer = DfsServer.getInstance(DfsServer.Runtime.CLUSTER_DFS);

  @ClassRule
  public static MrServer mrServer = MrServer.getInstance(MrServer.Runtime.CLUSTER_JOB);

  @Override
  public DfsServer getDfsServer() {
    return dfsServer;
  }

  @Override
  public MrServer getMrServer() {
    return mrServer;
  }

}
