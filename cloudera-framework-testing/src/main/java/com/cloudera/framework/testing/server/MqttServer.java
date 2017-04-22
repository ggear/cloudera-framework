package com.cloudera.framework.testing.server;

import static io.moquette.BrokerConstants.PERSISTENT_STORE_PROPERTY_NAME;
import static io.moquette.BrokerConstants.PORT_PROPERTY_NAME;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import io.moquette.server.Server;
import io.moquette.server.config.MemoryConfig;
import org.apache.commons.io.FileUtils;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MQTT Broker {@link TestRule}
 */
public class MqttServer extends CdhServer<MqttServer, MqttServer.Runtime> {

  public static final int MQTT_BROKER_PORT = 1883;
  public static final String MQTT_BROKER_STORE_NAME = "store";

  private static final Logger LOG = LoggerFactory.getLogger(MqttServer.class);

  private static MqttServer instance;

  private Server mqttServer;

  private MqttServer(Runtime runtime) {
    super(runtime);
  }

  /**
   * Get instance with default runtime
   *
   */
  public static synchronized MqttServer getInstance() {
    return getInstance(instance == null ? Runtime.LOCAL_BROKER : instance.getRuntime());
  }

  /**
   * Get instance with specific <code>runtime</code>
   *
   */
  public static synchronized MqttServer getInstance(Runtime runtime) {
    return instance == null ? instance = new MqttServer(runtime) : instance.assertRuntime(runtime);
  }

  /**
   * Get connect {@link String}
   *
   */
  public String getConnectString() {
    return "tcp://localhost:" + MQTT_BROKER_PORT;
  }

  @Override
  public int getIndex() {
    return 60;
  }

  @Override
  public CdhServer<?, ?>[] getDependencies() {
    return new CdhServer<?, ?>[0];
  }

  @Override
  public synchronized void start() throws Exception {
    long time = log(LOG, "start");
    File mqttServerDirectory = new File(ABS_DIR_MQTT);
    FileUtils.deleteDirectory(mqttServerDirectory);
    mqttServerDirectory.mkdirs();
    mqttServer = new Server();
    Properties mqttServerProperties = new Properties();
    mqttServerProperties.put(PERSISTENT_STORE_PROPERTY_NAME, ABS_DIR_MQTT + File.separatorChar + MQTT_BROKER_STORE_NAME);
    mqttServerProperties.put(PORT_PROPERTY_NAME, "" + MQTT_BROKER_PORT);
    mqttServer.startServer(new MemoryConfig(mqttServerProperties));
    log(LOG, "start", time);
  }

  @Override
  public synchronized void stop() throws IOException {
    long time = log(LOG, "stop");
    if (mqttServer != null) {
      mqttServer.stopServer();
    }
    log(LOG, "stop", time);
  }

  public enum Runtime {
    LOCAL_BROKER // Local file system backed MQTT broker, single-process, light-weight
  }

}
