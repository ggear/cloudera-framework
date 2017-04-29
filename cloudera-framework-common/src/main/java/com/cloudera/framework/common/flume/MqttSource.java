package com.cloudera.framework.common.flume;

import java.io.File;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractPollableSource;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MQTT {@link org.apache.flume.Source}
 */
public class MqttSource extends AbstractPollableSource {

  public static final String CONFIG_USER_NAME = "userName";
  public static final String CONFIG_USER_NAME_DEFAULT = "";
  public static final String CONFIG_PASSWORD_FILE = "passwordFile";
  public static final String CONFIG_PASSWORD_FILE_DEFAULT = "";
  public static final String CONFIG_PROVIDER_URL = "providerURL";
  public static final String CONFIG_PROVIDER_URL_DEFAULT = "tcp://localhost:1883";
  public static final String CONFIG_DESTINATION_NAME = "destinationName";
  public static final String CONFIG_DESTINATION_NAME_DEFAULT = "";
  private static final Logger LOG = LoggerFactory.getLogger(MqttSource.class);
  private String providerUrl;
  private String destinationName;

  private MqttClient client;
  private MqttConnectOptions clientOptions;
  private SourceCounter sourceCounter;

  @Override
  protected void doConfigure(Context context) throws FlumeException {
    sourceCounter = new SourceCounter(getName());
    providerUrl = context.getString(CONFIG_PROVIDER_URL, CONFIG_PROVIDER_URL_DEFAULT).trim();
    destinationName = context.getString(CONFIG_DESTINATION_NAME, CONFIG_DESTINATION_NAME_DEFAULT).trim();
    String userName = context.getString(CONFIG_USER_NAME, CONFIG_USER_NAME_DEFAULT).trim();
    String passwordFile = context.getString(CONFIG_PASSWORD_FILE, CONFIG_PASSWORD_FILE_DEFAULT).trim();
    try {
      clientOptions = new MqttConnectOptions();
      if (!userName.isEmpty()) {
        clientOptions.setUserName(userName);
        clientOptions.setPassword((passwordFile.isEmpty() ? "" : Files.toString(new File(passwordFile), Charsets.UTF_8).trim()).toCharArray());
      }
      clientOptions.setCleanSession(false);
      clientOptions.setAutomaticReconnect(false);
      clientOptions.setConnectionTimeout(Math.toIntExact(getBackOffSleepIncrement() / 2000));
      clientOptions = new MqttConnectOptions();
      client = new MqttClient(providerUrl, getName(), new MemoryPersistence());
    } catch (Exception e) {
      throw new FlumeException("Could not create MQTT client with broker [" + providerUrl + "] and client ID [" + getName() + "]", e);
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("MQTT client configured with broker [" + providerUrl + "], topic [" + destinationName + "] and client ID [" + getName() + "]");
    }
  }

  @Override
  protected synchronized void doStart() throws FlumeException {
    if (client != null) {
      try {
        if (!client.isConnected()) {
          client.connect(clientOptions);
          client.setCallback(new MqttCallback() {

            @Override
            public void connectionLost(Throwable cause) {
              if (LOG.isErrorEnabled()) {
                LOG.error("MQTT client disconnected from broker [" + providerUrl + "]", cause);
              }
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
              if (LOG.isDebugEnabled()) {
                LOG.debug("MQTT client received message from broker [" + providerUrl + "] and topic [" + destinationName + "] of size [" +
                  message.getPayload().length + "]");
              }
              try {
                Event event = new SimpleEvent();
                event.setBody(message.getPayload());
                sourceCounter.incrementAppendReceivedCount();
                sourceCounter.incrementEventReceivedCount();
                getChannelProcessor().processEvent(event);
                sourceCounter.incrementEventAcceptedCount();
                sourceCounter.incrementAppendAcceptedCount();
              } catch (Exception e) {
                if (LOG.isWarnEnabled()) {
                  LOG.warn("Failed to push message to channel(s), rolling back MQTT client transaction, disconnecting and backing-off MQTT client", e);
                }
                throw e;
              }
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
            }

          });
          if (LOG.isInfoEnabled()) {
            LOG.info("MQTT client connected to broker [" + providerUrl + "], subscribing to topic [" + destinationName + "]");
          }
          client.subscribe(destinationName);
        }
      } catch (MqttException e) {
        throw new FlumeException("Could not connect to MQTT broker [" + providerUrl + "]", e);
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
          LOG.info("MQTT client disconnecting from broker [" + providerUrl + "]");
        }
      } catch (MqttException e) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Could not disconnect from MQTT broker [" + providerUrl + "]", e);
        }
      }
    }
  }

  @Override
  protected synchronized Status doProcess() throws EventDeliveryException {
    if (client != null) {
      boolean connected = client.isConnected();
      doStart();
      if (connected && client.isConnected()) {
        try {
          Thread.sleep(getMaxBackOffSleepInterval());
        } catch (InterruptedException e) {
          // ignore
        }
      }
      return client.isConnected() ? Status.READY : Status.BACKOFF;
    }
    return Status.READY;
  }

}
