package com.cloudera.framework.common.flume;

import java.io.File;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractPollableSource;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MQTT {@link org.apache.flume.Source}
 */
public class MqttSource extends AbstractPollableSource {

  public static final String CONFIG_BROKER_ACCESS = "brokerAccess";
  public static final String CONFIG_BROKER_ACCESS_DEFAULT = "";
  public static final String CONFIG_BROKER_SECRET = "brokerSecret";
  public static final String CONFIG_BROKER_SECRET_DEFAULT = "";
  public static final String CONFIG_JOURNAL_DIR = "journalDir";
  public static final String CONFIG_JOURNAL_DIR_DEFAULT = "/tmp";
  public static final String CONFIG_PROVIDER_URL = "providerURL";
  public static final String CONFIG_PROVIDER_URL_DEFAULT = "tcp://localhost:1883";
  public static final String CONFIG_DESTINATION_NAME = "destinationName";
  public static final String CONFIG_DESTINATION_NAME_DEFAULT = "";

  private static final Logger LOG = LoggerFactory.getLogger(MqttSource.class);

  private String journalDir;
  private String providerUrl;
  private String destinationName;

  private MqttClient client;
  private MqttConnectOptions clientOptions;
  private SourceCounter sourceCounter;
  private ArrayBlockingQueue<Event> queue;

  @Override
  protected void doConfigure(Context context) throws FlumeException {
    queue = new ArrayBlockingQueue<>(1);
    sourceCounter = new SourceCounter(getName());
    journalDir = context.getString(CONFIG_JOURNAL_DIR, CONFIG_JOURNAL_DIR_DEFAULT).trim();
    providerUrl = context.getString(CONFIG_PROVIDER_URL, CONFIG_PROVIDER_URL_DEFAULT).trim();
    destinationName = context.getString(CONFIG_DESTINATION_NAME, CONFIG_DESTINATION_NAME_DEFAULT).trim();
    String brokerAccess = context.getString(CONFIG_BROKER_ACCESS, CONFIG_BROKER_ACCESS_DEFAULT).trim();
    String brokerSecret = context.getString(CONFIG_BROKER_SECRET, CONFIG_BROKER_SECRET_DEFAULT).trim();
    try {
      clientOptions = new MqttConnectOptions();
      if (!brokerAccess.isEmpty()) {
        clientOptions.setUserName(brokerAccess);
      }
      if (!brokerSecret.isEmpty()) {
        clientOptions.setPassword(brokerSecret.toCharArray());
      }
      clientOptions.setCleanSession(false);
      clientOptions.setAutomaticReconnect(false);
      clientOptions.setConnectionTimeout(Math.toIntExact(getBackOffSleepIncrement() / 2000));
    } catch (Exception e) {
      throw new FlumeException("MQTT client configure failed with [" + providerUrl + "/" + destinationName + "]", e);
    }
  }

  @Override
  protected synchronized void doStart() throws FlumeException {
    if (client == null) {
      try {
        new File(journalDir).mkdirs();
        client = new MqttClient(providerUrl, getName(), new MqttDefaultFilePersistence(journalDir));
        if (LOG.isInfoEnabled()) {
          LOG.info("MQTT client created with [" + providerUrl + "/" + destinationName + "]");
        }
      } catch (Exception e) {
        throw new FlumeException("MQTT client create failed with [" + providerUrl + "/" + destinationName + "]", e);
      }
    }
  }

  @Override
  protected synchronized void doStop() throws FlumeException {
    if (client != null) {
      try {
        if (client.isConnected()) {
          client.disconnect();
        }
        if (LOG.isInfoEnabled()) {
          LOG.info("MQTT client disconnecting from [" + providerUrl + "/" + destinationName + "]");
        }
      } catch (MqttException e) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("MQTT client could not disconnect from [" + providerUrl + "/" + destinationName + "]", e);
        }
      }
    }
  }

  @Override
  protected synchronized Status doProcess() throws EventDeliveryException {
    if (client != null) {
      if (!client.isConnected()) {
        try {
          client.connect(clientOptions);
          client.subscribe(destinationName, 2, new IMqttMessageListener() {
            public void messageArrived(String topic, MqttMessage message) throws Exception {
              if (LOG.isDebugEnabled()) {
                LOG.debug("MQTT client received message from [" +
                  providerUrl + "/" + topic + "] of size [" + message.getPayload().length + "]");
              }
              sourceCounter.incrementAppendReceivedCount();
              sourceCounter.incrementEventReceivedCount();
              getChannelProcessor().processEvent(EventBuilder.withBody(message.getPayload()));
              sourceCounter.incrementEventAcceptedCount();
              sourceCounter.incrementAppendAcceptedCount();
              if (LOG.isDebugEnabled()) {
                LOG.debug("MQTT client committed message to channels [" +
                  getChannelProcessor().getSelector().getAllChannels().stream().map(channel ->
                    channel.getName()).collect(Collectors.joining(",")) + "]");
              }
            }
          });
          if (LOG.isInfoEnabled()) {
            LOG.info("MQTT client connected to [" + providerUrl + "/" + destinationName + "]");
          }
          return Status.READY;
        } catch (MqttException e) {
          if (LOG.isErrorEnabled()) {
            LOG.error("MQTT client failed to connect to [" + providerUrl + "/" + destinationName + "]", e);
          }
        }
      }
    }
    return Status.BACKOFF;
  }

}
