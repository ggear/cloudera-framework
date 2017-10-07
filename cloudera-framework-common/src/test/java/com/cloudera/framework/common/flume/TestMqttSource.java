package com.cloudera.framework.common.flume;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.FlumeServer;
import com.cloudera.framework.testing.server.MqttServer;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.IOUtils;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.sink.hdfs.HDFSEventSink;
import org.apache.hadoop.fs.Path;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(TestRunner.class)
public class TestMqttSource implements TestConstants {

  @ClassRule
  public static final MqttServer mqttServer = MqttServer.getInstance();

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static final FlumeServer flumeServer = FlumeServer.getInstance();

  private static final String TOPIC_NAME_TEST = "test-topic";

  private static final int FLUME_CRANKS = 3;
  private static final int FLUME_CRANKS_MESSAGES = 3;

  private MqttClient client;

  @Before
  public void mqttClient() throws MqttException {
    client = new MqttClient(mqttServer.getConnectString(), UUID.randomUUID().toString(), new MemoryPersistence());
    client.connect();
  }

  @SuppressWarnings("SameReturnValue")
  private int mqttClientSendMessage(Integer iteration) {
    try {
      for (int i = 0; i < FLUME_CRANKS_MESSAGES; i++) {
        client.publish(TOPIC_NAME_TEST, UUID.randomUUID().toString().getBytes(), 0, false);
      }
    } catch (MqttException e) {
      throw new RuntimeException("Could not publish message", e);
    }
    return FLUME_CRANKS_MESSAGES;
  }

  @After
  public void mqttClientDisconnect() throws MqttException {
    client.disconnect();
  }

  @Test
  public void testMqttSource() throws MqttException, InterruptedException, IOException, EventDeliveryException {
    assertEquals(1,
      flumeServer.crankPipeline(
        ImmutableMap.of("HDFS_ROOT", dfsServer.getPathUri("/"), "TOPIC_NAME", TOPIC_NAME_TEST),
        "flume/flume-conf.properties", Collections.emptyMap(), Collections.emptyMap(),
        "agent", "mqtt", "hdfs", new MqttSource(), new HDFSEventSink(),
        "/tmp/flume-mqtt", FLUME_CRANKS, this::mqttClientSendMessage));
    int messageCount = 0;
    for (Path path : dfsServer.listFilesDfs("/tmp/flume-mqtt", true)) {
      messageCount += IOUtils.toString(dfsServer.getFileSystem().open(path)).split("\n").length;
    }
    assertEquals(FLUME_CRANKS * FLUME_CRANKS_MESSAGES, messageCount);
  }

}
