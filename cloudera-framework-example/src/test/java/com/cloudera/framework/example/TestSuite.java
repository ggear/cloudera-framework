package com.cloudera.framework.example;

import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.cloudera.framework.example.test.Process;
import com.cloudera.framework.example.test.Partition;
import com.cloudera.framework.example.test.Cleanse;
import com.cloudera.framework.example.test.Stage;
import com.cloudera.framework.example.test.Stream;
import com.cloudera.framework.example.test.Table;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.HiveServer;
import com.cloudera.framework.testing.server.MrServer;

@RunWith(Suite.class)
@SuiteClasses({ //
    Stream.class, //
    Stage.class, //
    Partition.class, //
    Cleanse.class, //
    Process.class, //
    Table.class, //
})

public class TestSuite {

  @ClassRule
  public static TestRule cdhServers = RuleChain //
      .outerRule(DfsServer.getInstance(DfsServer.Runtime.CLUSTER_DFS)) //
      .around(MrServer.getInstance(MrServer.Runtime.CLUSTER_JOB)) //
      .around(HiveServer.getInstance(HiveServer.Runtime.CLUSTER_MR2));

}
