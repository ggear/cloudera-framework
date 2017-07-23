package com.cloudera;

import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.HiveServer;
import com.cloudera.framework.testing.server.MrServer;
import com.cloudera.framework.testing.server.PythonServer;
import com.cloudera.test.Hive;
import com.cloudera.test.Python;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * Test suite that bootstraps all unit-test classes with the necessary,
 * heavy-weight, CDH runtime servers (overriding the unit-test classes
 * light-weight runtimes)
 */
@RunWith(Suite.class)
@SuiteClasses({ //
  Hive.class, //
  Python.class, //
})
public class TestSuite {

  @ClassRule
  public static TestRule cdhServers = RuleChain //
    .outerRule(DfsServer.getInstance(DfsServer.Runtime.CLUSTER_DFS)) //
    .around(MrServer.getInstance(MrServer.Runtime.CLUSTER_JOB)) //
    .around(HiveServer.getInstance(HiveServer.Runtime.LOCAL_MR2)) //
    .around(PythonServer.getInstance(PythonServer.Runtime.LOCAL_CPYTHON27));

}
