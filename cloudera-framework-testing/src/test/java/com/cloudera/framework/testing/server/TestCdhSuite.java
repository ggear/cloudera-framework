package com.cloudera.framework.testing.server;

import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.cloudera.framework.testing.server.tests.TestDfsServerDefault;
import com.cloudera.framework.testing.server.tests.TestFlumeServer;
import com.cloudera.framework.testing.server.tests.TestHiveServerMrDefault;
import com.cloudera.framework.testing.server.tests.TestKafkaServer;
import com.cloudera.framework.testing.server.tests.TestKuduServer;
import com.cloudera.framework.testing.server.tests.TestMrServerDefault;
import com.cloudera.framework.testing.server.tests.TestSparkServerDefault;
import com.cloudera.framework.testing.server.tests.TestZooKeeperServer;

@RunWith(Suite.class)
@SuiteClasses({ //
    TestDfsServerDefault.class, //
    TestKuduServer.class, //
    TestZooKeeperServer.class, //
    TestKafkaServer.class, //
    TestFlumeServer.class, //
    TestMrServerDefault.class, //
    TestSparkServerDefault.class, //
    TestHiveServerMrDefault.class, //
})

public class TestCdhSuite {

  @ClassRule
  public static TestRule cdhServers = RuleChain //
      .outerRule(DfsServer.getInstance()) //
      .around(KuduServer.getInstance()) //
      .around(ZooKeeperServer.getInstance()) //
      .around(KafkaServer.getInstance()) //
      .around(FlumeServer.getInstance()) //
      .around(MrServer.getInstance()) //
      .around(SparkServer.getInstance()) //
      .around(HiveServer.getInstance());

}
