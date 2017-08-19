package com.cloudera.framework.example.one;

import com.cloudera.framework.example.one.test.Cleanse;
import com.cloudera.framework.example.one.test.Partition;
import com.cloudera.framework.example.one.test.Process;
import com.cloudera.framework.example.one.test.Stage;
import com.cloudera.framework.example.one.test.Stream;
import com.cloudera.framework.example.one.test.Table;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.HiveServer;
import com.cloudera.framework.testing.server.MrServer;
import com.cloudera.framework.testing.server.PythonServer;
import com.cloudera.framework.testing.server.PythonServer.Runtime;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

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
    .around(PythonServer.getInstance(Runtime.LOCAL_CPYTHON_2_7)) //
    .around(HiveServer.getInstance(HiveServer.Runtime.LOCAL_MR2));

}
