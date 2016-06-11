package com.cloudera.example;

import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.cloudera.example.test.Ingest;
import com.cloudera.example.test.Partition;
import com.cloudera.example.test.Process;
import com.cloudera.example.test.Stage;
import com.cloudera.example.test.Stream;
import com.cloudera.example.test.Table;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.HiveServer;
import com.cloudera.framework.testing.server.MrServer;

@RunWith(Suite.class)
@SuiteClasses({ //
    Stream.class, //
    Stage.class, //
    Partition.class, //
    Process.class, //
    Ingest.class, //
    Table.class, //
})

public class TestSuite {

  @ClassRule
  public static TestRule cdhServers = RuleChain //
      .outerRule(DfsServer.getInstance(DfsServer.Runtime.CLUSTER_DFS)) //
      .around(MrServer.getInstance(MrServer.Runtime.CLUSTER_JOB)) //
      .around(HiveServer.getInstance(HiveServer.Runtime.CLUSTER_MR2));

}
