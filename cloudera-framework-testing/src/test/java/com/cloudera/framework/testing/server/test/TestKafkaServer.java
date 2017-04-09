package com.cloudera.framework.testing.server.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.KafkaServer;
import com.cloudera.framework.testing.server.ZooKeeperServer;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(TestRunner.class)
public class TestKafkaServer implements TestConstants {

  private static final String TOPIC_NAME_TEST = "mytopic";
  @ClassRule
  public static KafkaServer kafkaServer = KafkaServer.getInstance();

  @Test
  public void testImplicitDependencies() {
    assertTrue(ZooKeeperServer.getInstance().isStarted());
  }

  @Test
  public void testKafka() throws InterruptedException, IOException, ExecutionException, TimeoutException {
    int pollCount = 5;
    int messageCount = 10;
    kafkaServer.createTopic(TOPIC_NAME_TEST, 1, 1, new Properties());
    Producer<String, String> producer = new KafkaProducer<>(kafkaServer.getProducerProperties());
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaServer.getConsumerProperties());
    try {
      consumer.subscribe(Arrays.asList(TOPIC_NAME_TEST));
      for (int i = 0; i < messageCount; i++) {
        producer.send(new ProducerRecord<>(TOPIC_NAME_TEST, "" + i, "" + i)).get(KafkaServer.KAFKA_POLL_MS, TimeUnit.MILLISECONDS);
      }
      producer.flush();
      ConsumerRecords<String, String> records = null;
      while ((records = consumer.poll(KafkaServer.KAFKA_POLL_MS * messageCount)).count() == 0) {
        if (pollCount-- == 0) {
          throw new TimeoutException("Could not poll message batch");
        }
      }
      assertEquals(messageCount, records.count());
      for (ConsumerRecord<String, String> record : records) {
        assertTrue(record.offset() >= Long.parseLong(record.key()));
        assertTrue(Long.parseLong(record.key()) == Long.parseLong(record.value()));
      }
    } finally {
      IOUtils.closeQuietly(producer);
      IOUtils.closeQuietly(consumer);
    }
  }

  @Test
  public void testKafkaAgain() throws InterruptedException, IOException, ExecutionException, TimeoutException {
    testKafka();
  }

}
