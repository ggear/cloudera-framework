package com.cloudera.framework.main.test.cluster;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.framework.main.test.BaseTest;

import kafka.admin.AdminUtils;
import kafka.common.TopicExistsException;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZKStringSerializer$;

/**
 * Provide a mini-Kafka cluster, requires an externally managed mini-ZooKeeper
 * instance
 */
public class MiniKafka {

  public static final int KAFKA_POLL_MS = 250;

  private static final File DIR_DATA = new File(BaseTest.ABS_DIR_TARGET + "/test-kafka");

  private static Logger LOG = LoggerFactory.getLogger(MiniKafka.class);

  private MiniZooKeeper zooKeeper;
  private ZkClient zooKeeperClient;
  private KafkaServerStartable kafka;
  private Producer<String, String> producer;

  public MiniKafka(MiniZooKeeper zooKeeper) throws IOException, InterruptedException {
    this.zooKeeper = zooKeeper;
  }

  public void start() throws IOException, InterruptedException {
    FileUtils.deleteDirectory(DIR_DATA);
    Properties properties = new Properties();
    properties.put("log.dir", DIR_DATA.getAbsolutePath());
    properties.put("zookeeper.connect", zooKeeper.getConnectString());
    properties.put("broker.id", "1");
    kafka = new KafkaServerStartable(new KafkaConfig(properties));
    kafka.startup();
    properties.clear();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getConnectString());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producer = new KafkaProducer<String, String>(properties);
    zooKeeperClient = new ZkClient(zooKeeper.getConnectString(), MiniZooKeeper.ZOOKEEPER_TIMEOUT_MS,
        MiniZooKeeper.ZOOKEEPER_TIMEOUT_MS, ZKStringSerializer$.MODULE$);
  }

  public void stop() throws IOException {
    zooKeeperClient.close();
    producer.close();
    kafka.shutdown();
    kafka.awaitShutdown();
  }

  public String getConnectString() throws IOException {
    if (kafka == null) {
      throw new IOException("Kafka not started yet, port not allocated");
    }
    return HostNetwork.SERVER_BIND_IP + ":" + kafka.serverConfig().port();
  }

  public void create(String topic) throws InterruptedException {
    try {
      AdminUtils.createTopic(zooKeeperClient, topic, 1, 1, new Properties());
    } catch (TopicExistsException e) {
      // ignore
    }
    while (AdminUtils.fetchTopicMetadataFromZk(topic, zooKeeperClient).toString()
        .contains("LeaderNotAvailableException")) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Sleeping for [" + KAFKA_POLL_MS + "] ms, waiting for Kafka topic to be reigstered in ZK");
      }
      Thread.sleep(KAFKA_POLL_MS);
    }
  }

  public void send(String topic, String key, String value) {
    producer.send(new ProducerRecord<String, String>(topic, key, value));
  }

  public void delete(String topic) throws InterruptedException {
    AdminUtils.deleteTopic(zooKeeperClient, topic);
  }

}
