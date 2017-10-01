package com.cloudera.framework.common.flume;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.event.EventBuilder;
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

  public static final String CONFIG_BROKER_ACCESS = "brokerAccess";
  public static final String CONFIG_BROKER_ACCESS_DEFAULT = "";
  public static final String CONFIG_BROKER_SECRET = "brokerSecret";
  public static final String CONFIG_BROKER_SECRET_DEFAULT = "";
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
  private ArrayBlockingQueue<Event> queue;

  @Override
  protected void doConfigure(Context context) throws FlumeException {
    queue = new ArrayBlockingQueue<>(1);
    sourceCounter = new SourceCounter(getName());
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
      throw new FlumeException("Could not create MQTT client with broker [" + providerUrl + "] and client ID [" + getName() + "]", e);
    }
  }

  @Override
  protected synchronized void doStart() throws FlumeException {
    if (client == null) {
      try {
        client = new MqttClient(providerUrl, getName(), new MemoryPersistence());
        if (LOG.isInfoEnabled()) {
          LOG.info("MQTT client configured with broker [" + providerUrl + "] with user [" + clientOptions.getUserName() + "], topic [" +
            destinationName + "] and client ID [" + getName() + "]");
        }
      } catch (Exception e) {
        throw new FlumeException("Could not create MQTT client with broker [" + providerUrl + "] and client ID [" + getName() + "]", e);
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
      if (!client.isConnected()) {
        try {
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
                LOG.debug("MQTT client received message from broker [" + providerUrl + "] and topic [" +
                  topic + "] of size [" + message.getPayload().length + "]");
              }
              try {
                queue.offer(EventBuilder.withBody(message.getPayload()), getBackOffSleepIncrement(), TimeUnit.SECONDS);
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Committed event to channel queue");
                }
              } catch (Exception e) {
                if (LOG.isWarnEnabled()) {
                  LOG.warn("Failed to commit event to channel queue", e);
                }
                throw e;
              }
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
            }

          });
          client.connect(clientOptions);
          client.subscribe(destinationName);
          if (LOG.isInfoEnabled()) {
            LOG.info("MQTT client connected to broker [" + providerUrl + "] with user [" + clientOptions.getUserName() + "], topic [" +
              destinationName + "] and client ID [" + getName() + "]");
          }
        } catch (MqttException e) {
          if (LOG.isErrorEnabled()) {
            LOG.error("Could not connect to MQTT broker [" + providerUrl + "] with user [" + clientOptions.getUserName() + "]", e);
          }
        }
      }
      if (client.isConnected()) {
        try {
          Event event = queue.poll(getBackOffSleepIncrement(), TimeUnit.SECONDS);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Polled event from channel queue");
          }
          sourceCounter.incrementAppendReceivedCount();
          sourceCounter.incrementEventReceivedCount();
          getChannelProcessor().processEvent(event);
          sourceCounter.incrementEventAcceptedCount();
          sourceCounter.incrementAppendAcceptedCount();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Committed event to channel");
          }
        } catch (InterruptedException e) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Channel queue empty, re-polling");
          }
        } catch (Exception e) {
          if (LOG.isWarnEnabled()) {
            LOG.warn("Failed to commit event to channel", e);
          }
          return Status.BACKOFF;
        }
        return Status.READY;
      }
    }
    return Status.BACKOFF;
  }

}
