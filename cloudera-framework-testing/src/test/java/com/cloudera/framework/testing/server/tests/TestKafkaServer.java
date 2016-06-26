package com.cloudera.framework.testing.server.tests;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.KafkaServer;
import com.cloudera.framework.testing.server.ZooKeeperServer;

import kafka.admin.AdminUtils;

@RunWith(TestRunner.class)
public class TestKafkaServer implements TestConstants {

  @ClassRule
  public static KafkaServer kafkaServer = KafkaServer.getInstance();

  @Test
  public void testImplicitDependencies() {
    assertTrue(ZooKeeperServer.getInstance().isStarted());
  }

  @Test
  public void testKafka() throws InterruptedException, IOException {
    String topic = "mytesttopic";
    AdminUtils.createTopic(kafkaServer.getZooKeeperUtils(), topic, 1, 1, new Properties());
    // TODO: Add new consumer written against new Kafka 0.9.0 consumer API,
    // testing that messages are received, as simply and cleanly as possible
    // likely using global java.util.concurrent.CountDownLatch's for threading
    Producer<String, String> producer = new KafkaProducer<>(kafkaServer.getProducerProperties());
    try {
      producer.send(new ProducerRecord<>(topic, "my-key", "my-value"));
    } finally {
      IOUtils.closeQuietly(producer);
    }
  }

  @Test
  @Ignore // TODO: Remove when KafkaServer.clean() is implemented and topics are
          // flushed between test methods and this test can pass
  public void testKafkaAgain() throws InterruptedException, IOException {
    testKafka();
  }

}
