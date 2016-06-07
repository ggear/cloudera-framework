package com.cloudera.framework.testing.server;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.admin.AdminUtils;
import kafka.common.TopicAlreadyMarkedForDeletionException;
import kafka.common.TopicExistsException;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

/**
 * Kafka {@link TestRule}
 */
public class KafkaServer extends CdhServer<KafkaServer, KafkaServer.Runtime> {

  public enum Runtime {
    CLUSTER_BROKER
  };

  public static synchronized KafkaServer getInstance() {
    return getInstance(instance == null ? Runtime.CLUSTER_BROKER : instance.getRuntime());
  }

  public static synchronized KafkaServer getInstance(Runtime runtime) {
    return instance == null ? instance = new KafkaServer(runtime) : instance.assertRuntime(runtime);
  }

  public synchronized String getConnectString() throws IOException {
    if (kafka == null) {
      throw new IOException("Kafka not started yet, port not allocated");
    }
    return CdhServer.SERVER_BIND_IP + ":" + kafka.serverConfig().port();
  }

  public synchronized void create(String topic) throws InterruptedException {
    try {
      AdminUtils.createTopic(zooKeeperUtils, topic, 1, 1, new Properties());
    } catch (TopicExistsException e) {
      // ignore
    }
    while (AdminUtils.fetchTopicMetadataFromZk(topic, zooKeeperUtils).toString()
        .contains("LeaderNotAvailableException")) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Sleeping for [" + KAFKA_POLL_MS + "] ms, waiting for Kafka topic to be reigstered in ZK");
      }
      Thread.sleep(KAFKA_POLL_MS);
    }
  }

  public synchronized void send(String topic, String key, String value) {
    producer.send(new ProducerRecord<>(topic, key, value));
  }

  public synchronized void delete(String topic) throws InterruptedException {
    try {
      AdminUtils.deleteTopic(zooKeeperUtils, topic);
    } catch (TopicAlreadyMarkedForDeletionException exception) {
      // ignore
    }
  }

  @Override
  public int getIndex() {
    return 60;
  }

  @Override
  public CdhServer<?, ?>[] getDependencies() {
    return new CdhServer<?, ?>[] { ZooKeeperServer.getInstance() };
  }

  @Override
  public synchronized void start() throws Exception {
    long time = log(LOG, "start");
    FileUtils.deleteDirectory(new File(ABS_DIR_KAFKA));
    Properties properties = new Properties();
    properties.put("log.dir", ABS_DIR_KAFKA);
    properties.put("zookeeper.connect", ZooKeeperServer.getInstance().getConnectString());
    properties.put("broker.id", "1");
    kafka = new KafkaServerStartable(new KafkaConfig(properties));
    kafka.startup();
    properties.clear();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getConnectString());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producer = new KafkaProducer<>(properties);
    zooKeeperClient = new ZkClient(ZooKeeperServer.getInstance().getConnectString(),
        ZooKeeperServer.ZOOKEEPER_TIMEOUT_MS, ZooKeeperServer.ZOOKEEPER_TIMEOUT_MS, ZKStringSerializer$.MODULE$);
    zooKeeperUtils = new ZkUtils(zooKeeperClient, new ZkConnection(ZooKeeperServer.getInstance().getConnectString()),
        false);
    log(LOG, "start", time);
  }

  @Override
  public synchronized void clean() throws Exception {
    long time = log(LOG, "clean");
    // TODO: Provide impl
    if (LOG.isWarnEnabled()) {
      LOG.warn(logPrefix() + " [clean] not implemented");
    }
    log(LOG, "clean", time);
  }

  @Override
  public synchronized void state() throws Exception {
    long time = log(LOG, "state", true);
    // TODO: Provide impl
    if (LOG.isWarnEnabled()) {
      LOG.warn(logPrefix() + " [state] not implemented");
    }
    log(LOG, "state", time, true);
  }

  @Override
  public synchronized void stop() {
    long time = log(LOG, "stop");
    if (zooKeeperUtils != null) {
      zooKeeperUtils.close();
    }
    if (zooKeeperClient != null) {
      zooKeeperClient.close();
    }
    if (producer != null) {
      producer.close();
    }
    if (kafka != null) {
      kafka.shutdown();
      kafka.awaitShutdown();
    }
    log(LOG, "stop", time);
  }

  private static final Logger LOG = LoggerFactory.getLogger(KafkaServer.class);

  private static final int KAFKA_POLL_MS = 250;

  private static KafkaServer instance;

  private ZkUtils zooKeeperUtils;
  private ZkClient zooKeeperClient;
  private KafkaServerStartable kafka;
  private Producer<String, String> producer;

  private KafkaServer(Runtime runtime) {
    super(runtime);
  }

}
