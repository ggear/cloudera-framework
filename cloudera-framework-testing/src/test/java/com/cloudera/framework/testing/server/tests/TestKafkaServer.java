package com.cloudera.framework.testing.server.tests;

import static org.junit.Assert.assertTrue;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.KafkaServer;
import com.cloudera.framework.testing.server.ZooKeeperServer;

@RunWith(TestRunner.class)
public class TestKafkaServer implements TestConstants {

  @ClassRule
  public static KafkaServer kafkaServer = KafkaServer.getInstance();

  @Test
  public void testImplicitDependencies() {
    assertTrue(ZooKeeperServer.getInstance().isStarted());
  }

  @Test
  public void testKafka() throws InterruptedException {
    String topic = "mytesttopic";
    kafkaServer.create(topic);
    kafkaServer.send(topic, "my-key", "my-value");

    // TODO: Add new consumer due with Kafka 0.9.0

    kafkaServer.delete(topic);
  }

  @Test
  public void testKafkaAgain() throws InterruptedException {
    testKafka();
  }

}
