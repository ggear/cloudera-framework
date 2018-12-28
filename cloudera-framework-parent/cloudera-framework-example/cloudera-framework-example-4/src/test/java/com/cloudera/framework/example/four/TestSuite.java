package com.cloudera.framework.example.four;

import com.cloudera.framework.example.four.test.Stream;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.KafkaServer;
import com.cloudera.framework.testing.server.SparkServer;

import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
  Stream.class
})

public class TestSuite {

  @ClassRule
  public static TestRule cdhServers = RuleChain
    .outerRule(DfsServer.getInstance(DfsServer.Runtime.CLUSTER_DFS))
    .around(KafkaServer.getInstance())
    .around(SparkServer.getInstance())
    .around(SparkServer.getInstance());

}
