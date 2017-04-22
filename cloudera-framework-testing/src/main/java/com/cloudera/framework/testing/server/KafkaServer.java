package com.cloudera.framework.testing.server;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.common.TopicAlreadyMarkedForDeletionException;
import kafka.common.TopicExistsException;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;

/**
 * Kafka {@link TestRule}
 */
public class KafkaServer extends CdhServer<KafkaServer, KafkaServer.Runtime> {

  public static final int KAFKA_POLL_MS = 100;

  private static final Logger LOG = LoggerFactory.getLogger(KafkaServer.class);
  private static final String TOPIC_CONSUMER_OFFSETS = "__consumer_offsets";

  private static KafkaServer instance;

  private ZkUtils zooKeeperUtils;
  private ZkClient zooKeeperClient;
  private KafkaServerStartable kafka;

  private KafkaServer(Runtime runtime) {
    super(runtime);
  }

  /**
   * Get instance with default runtime
   *
   */
  public static synchronized KafkaServer getInstance() {
    return getInstance(instance == null ? Runtime.CLUSTER_BROKER : instance.getRuntime());
  }

  /**
   * Get instance with specific <code>runtime</code>
   *
   */
  public static synchronized KafkaServer getInstance(Runtime runtime) {
    return instance == null ? instance = new KafkaServer(runtime) : instance.assertRuntime(runtime);
  }

  /**
   * Get connect string
   *
   * @return formatted as <code>host:post</code>
   */
  public synchronized String getConnectString() throws IOException {
    if (kafka == null) {
      throw new IOException("Kafka not started yet, port not allocated");
    }
    return CdhServer.SERVER_BIND_IP + ":" + kafka.serverConfig().port();
  }

  /**
   * Get the ZooKeeper Utils
   *
   */
  public synchronized ZkUtils getZooKeeperUtils() {
    return zooKeeperUtils;
  }

  /**
   * Create topic
   *
   * @param topic
   * @param partitions
   * @param replicationFactor
   * @param properties
   * @return if the topic was created or not
   */
  public synchronized boolean createTopic(String topic, int partitions, int replicationFactor, Properties properties)
    throws InterruptedException {
    boolean created = true;
    try {
      AdminUtils.createTopic(getZooKeeperUtils(), topic, partitions, replicationFactor, properties, AdminUtils.createTopic$default$6());
    } catch (TopicExistsException topicExistsException) {
      created = false;
    }
    while (AdminUtils.fetchTopicMetadataFromZk(topic, zooKeeperUtils).toString().contains("LeaderNotAvailableException")) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Sleeping for [" + KAFKA_POLL_MS + "] ms, waiting for Kafka topic to be registered in ZooKeeper");
      }
      Thread.sleep(KAFKA_POLL_MS);
    }
    return created;
  }

  /**
   * Delete topic
   *
   * @param topic
   * @return if the topic was deleted or not
   */
  public synchronized boolean deleteTopic(String topic) {
    boolean deleted = true;
    try {
      AdminUtils.deleteTopic(getZooKeeperUtils(), topic);
    } catch (TopicAlreadyMarkedForDeletionException topicAlreadyMarkedForDeletionException) {
      deleted = false;
    }
    return deleted && AdminUtils.topicExists(getZooKeeperUtils(), topic);
  }

  /**
   * Get standard producer properties
   *
   */
  public synchronized Properties getProducerProperties() throws IOException {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getConnectString());
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.RETRIES_CONFIG, "0");
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "1");
    properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
    properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "1024");
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return properties;
  }

  /**
   * Get standard consumer properties
   *
   */
  public synchronized Properties getConsumerProperties() throws IOException {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getConnectString());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-test");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    return properties;
  }

  @Override
  public int getIndex() {
    return 60;
  }

  @Override
  public CdhServer<?, ?>[] getDependencies() {
    return new CdhServer<?, ?>[]{ZooKeeperServer.getInstance()};
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
    zooKeeperClient = new ZkClient(ZooKeeperServer.getInstance().getConnectString(), ZooKeeperServer.ZOOKEEPER_TIMEOUT_MS,
      ZooKeeperServer.ZOOKEEPER_TIMEOUT_MS, ZKStringSerializer$.MODULE$);
    zooKeeperUtils = new ZkUtils(zooKeeperClient, new ZkConnection(ZooKeeperServer.getInstance().getConnectString()), false);
    log(LOG, "start", time);
  }

  @Override
  public synchronized void clean() throws Exception {
    long time = log(LOG, "clean");
    Iterator<String> topics = AdminUtils.fetchAllTopicConfigs(zooKeeperUtils).keys().iterator();
    while (topics.hasNext()) {
      String topic = topics.next();
      if (!topic.equals(TOPIC_CONSUMER_OFFSETS)) {
        deleteTopic(topic);
      }
    }
    log(LOG, "clean", time);
  }

  @Override
  public synchronized void state() throws Exception {
    long time = log(LOG, "state", true);
    StringBuilder topicsString = new StringBuilder();
    Iterator<String> topics = AdminUtils.fetchAllTopicConfigs(zooKeeperUtils).keys().iterator();
    while (topics.hasNext()) {
      String topic = topics.next();
      if (!topic.equals(TOPIC_CONSUMER_OFFSETS)) {
        topicsString.append("\n").append(topic);
      }
    }
    log(LOG, "state", "topics:" + topicsString, true);
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
    if (kafka != null) {
      kafka.shutdown();
      kafka.awaitShutdown();
    }
    log(LOG, "stop", time);
  }

  public enum Runtime {
    CLUSTER_BROKER // Kafka broker, multi-threaded, heavy-weight
  }

}
