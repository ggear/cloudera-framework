package com.cloudera.framework.example.stream;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.cloudera.framework.example.model.Record;
import com.cloudera.framework.example.model.RecordFactory;
import com.cloudera.framework.example.model.RecordKey;
import com.cloudera.framework.example.model.serde.RecordStringSerDe;
import com.cloudera.framework.example.model.serde.RecordStringSerDe.RecordStringSer;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.Source;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.sink.hdfs.SequenceFileSerializer;
import org.apache.flume.source.AbstractSource;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flume {@link Source} to generate dataset events
 */
public class Stream extends AbstractSource implements Configurable, PollableSource {

  public static final String HEADER_TIMESTAMP = "ts";
  public static final String HEADER_BATCH_ID = "bid";
  public static final String HEADER_BATCH_TYPE = "bt";
  public static final String HEADER_BATCH_INDEX = "bi";
  public static final String HEADER_BATCH_SUM = "bs";
  public static final String HEADER_BATCH_TIMESTAMP_START = "btss";
  public static final String HEADER_BATCH_TIMESTAMP_FINISH = "btsf";

  public static final String PROPERTY_POLL_MS = "pollMs";
  public static final String PROPERTY_POLL_TICKS = "pollTicks";
  public static final String PROPERTY_BATCH_SIZE = "batchSize";
  public static final String PROPERTY_RECORD_TYPE = "recordType";
  public static final String PROPERTY_RECORD_NUMBER = "recordNumber";

  public static final String AGENT_ID = UUID.randomUUID().toString();

  private static final String RECORD_TYPE_DEFAULT = RecordFactory.RECORD_STRING_SERDE_CSV;

  private static final Logger LOG = LoggerFactory.getLogger(Stream.class);

  private int pollMs = 1000;
  private int pollTicks = 0;
  private int batchSize = 1;
  private int recordNumber = 10;
  private String recordType = RECORD_TYPE_DEFAULT;
  private RecordStringSerDe recordStringSerDe;

  private List<Event> eventBatch;
  private SourceCounter sourceCounter;

  @Override
  public void configure(Context context) {
    pollMs = context.getInteger(PROPERTY_POLL_MS, pollMs);
    if (pollMs <= 0) {
      throw new IllegalArgumentException(
        "Source [" + getName() + "] has illegal paramater [" + PROPERTY_POLL_MS + "] value [" + pollMs + "]");
    }
    pollTicks = context.getInteger(PROPERTY_POLL_TICKS, pollTicks);
    if (pollTicks < 0) {
      throw new IllegalArgumentException(
        "Source [" + getName() + "] has illegal paramater [" + PROPERTY_POLL_TICKS + "] value [" + pollTicks + "]");
    }
    batchSize = context.getInteger(PROPERTY_BATCH_SIZE, batchSize);
    if (batchSize < 1) {
      throw new IllegalArgumentException(
        "Source [" + getName() + "] has illegal paramater [" + PROPERTY_BATCH_SIZE + "] value [" + batchSize + "]");
    }
    try {
      recordStringSerDe = RecordFactory.getRecordStringSerDe(recordType = context.getString(PROPERTY_RECORD_TYPE, recordType));
    } catch (IOException exception) {
      throw new IllegalArgumentException("Source [" + getName() + "] has illegal paramater [" + PROPERTY_RECORD_TYPE + "].", exception);
    }
    recordNumber = context.getInteger(PROPERTY_RECORD_NUMBER, recordNumber);
    if (recordNumber < 1) {
      throw new IllegalArgumentException(
        "Source [" + getName() + "] has illegal paramater [" + PROPERTY_RECORD_NUMBER + "] value [" + recordNumber + "]");
    }
    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("Source [" + getName() + "] configured, Agent ID [" + AGENT_ID + "], context [" + context + "]");
    }
  }

  @Override
  public synchronized void start() {
    super.start();
    eventBatch = new ArrayList<>();
    sourceCounter.start();
    if (LOG.isInfoEnabled()) {
      LOG.info("Source [" + getName() + "] started");
    }
  }

  @Override
  public synchronized void stop() {
    processEvent(null, true);
    sourceCounter.stop();
    super.stop();
    if (LOG.isInfoEnabled()) {
      LOG.info("Source [" + getName() + "] stopped, metrics [" + sourceCounter + "]");
    }
  }

  private Map<String, String> getEventHeader(long timestamp) {
    Map<String, String> header = new HashMap<>();
    header.put(HEADER_BATCH_TYPE, recordType);
    header.put(HEADER_TIMESTAMP, "" + timestamp);
    return header;
  }

  private synchronized void processEvent(Event event, boolean flush) {
    if (event != null) {
      eventBatch.add(event);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Source [" + getName() + "] batched event, buffered events [" + eventBatch.size() + "]");
      }
    }
    if (eventBatch.size() > 0 && (flush || eventBatch.size() == batchSize)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Source [" + getName() + "] pending commit, buffered events [" + eventBatch.size() + "]");
      }
      if (batchSize == 1 && eventBatch.size() == 1) {
        sourceCounter.incrementAppendReceivedCount();
        sourceCounter.incrementEventReceivedCount();
        getChannelProcessor().processEvent(eventBatch.get(0));
        sourceCounter.incrementAppendAcceptedCount();
        sourceCounter.incrementEventAcceptedCount();
      } else {
        sourceCounter.incrementAppendBatchReceivedCount();
        sourceCounter.addToEventReceivedCount(eventBatch.size());
        getChannelProcessor().processEventBatch(eventBatch);
        sourceCounter.incrementAppendBatchAcceptedCount();
        sourceCounter.addToEventAcceptedCount(eventBatch.size());
      }
      eventBatch.clear();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Source [" + getName() + "] post commit, buffered events [" + eventBatch.size() + "]");
      }
    }
  }

  @Override
  public Status process() throws EventDeliveryException {
    long time = System.currentTimeMillis();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Source [" + getName() + "] process started");
    }
    Status status = Status.BACKOFF;
    try {
      RecordStringSer recordStringSer = recordStringSerDe.getSerialiser(recordNumber);
      for (int i = 0; i < recordNumber; i++) {
        recordStringSer.add(Record.newBuilder().setMyTimestamp(System.currentTimeMillis()).setMyInteger((int) (Math.random() * 10))
          .setMyDouble(Math.round(Math.random() * 10000000) / 100D).setMyBoolean(Math.random() < 0.5 ? true : false)
          .setMyString(UUID.randomUUID().toString()).build());
      }
      String record = recordStringSer.get();
      processEvent(EventBuilder.withBody(record, Charset.forName(Charsets.UTF_8.name()), getEventHeader(System.currentTimeMillis())),
        false);
      int sleepMs = 0;
      int tickMs = pollMs / (pollTicks + 1);
      for (int i = 0; i <= pollTicks; i++) {
        if (pollTicks > 0 && i < pollTicks) {
          processEvent(EventBuilder.withBody(record, Charset.forName(Charsets.UTF_8.name()), getEventHeader(System.currentTimeMillis())),
            false);
          if (i < pollTicks - 1) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Source [" + getName() + "] sleeping for next tick, ms [" + tickMs + "]");
            }
          }
        }
        if (i == pollTicks) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Source [" + getName() + "] sleeping for next poll, ms [" + tickMs + "]");
          }
        }
        sleepMs += tickMs;
        Thread.sleep(tickMs);
        if (pollTicks > 0 && i < pollTicks - 1) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Source [" + getName() + "] waking up for next tick, ms [" + tickMs + "]");
          }
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Source [" + getName() + "] waking up for next poll, ms [" + sleepMs + "]");
      }
      status = Status.READY;
    } catch (InterruptedException interruptedException) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Source [" + getName() + "] interupted");
      }
    } catch (Error error) {
      throw error;
    } catch (Exception exception) {
      if (LOG.isErrorEnabled()) {
        LOG.error("Source [" + getName() + "] encountered exception processing event, " + "backing off and retrying", exception);
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Source [" + getName() + "] process stopped, ms [" + (System.currentTimeMillis() - time) + "]");
    }

    return status;
  }

  @Override
  public long getBackOffSleepIncrement() {
    // TODO: Drive by configuration
    return 50;
  }

  @Override
  public long getMaxBackOffSleepInterval() {
    // TODO: Drive by configuration
    return 200;
  }

  public static class Interceptor implements org.apache.flume.interceptor.Interceptor {

    private static final Logger LOG = LoggerFactory.getLogger(Interceptor.class);

    private String recordType;

    public Interceptor(String recordType) {
      this.recordType = recordType;
    }

    private static String putHeader(Event event, String key, String value) {
      return putHeader(event, key, value, false);
    }

    private static String putHeader(Event event, String key, String value, boolean force) {
      String valuePrevious = event.getHeaders().get(key);
      if (force || valuePrevious == null) {
        event.getHeaders().put(key, value);
        if (LOG.isTraceEnabled()) {
          LOG.trace("Adding event header [" + key + "] with value [" + value + "]"
            + (valuePrevious == null ? "" : " overwriting previous value [" + valuePrevious + "]"));
        }
      }
      return force || valuePrevious == null ? value : valuePrevious;
    }

    @Override
    public void initialize() {
      if (LOG.isInfoEnabled()) {
        LOG.info("Stream Interceptor initialised, Agent ID [" + AGENT_ID + "]");
      }
    }

    @Override
    public Event intercept(Event event) {
      String timestamp = putHeader(event, HEADER_TIMESTAMP, "" + System.currentTimeMillis());
      return setEventHeaders(event, timestamp, UUID.randomUUID().toString(), 1, 1, timestamp, timestamp);
    }

    @Override
    public List<Event> intercept(List<Event> events) {
      String timestamp = "" + System.currentTimeMillis();
      String batchId = UUID.randomUUID().toString();
      String batchTimestampStart = putHeader(events.get(0), HEADER_TIMESTAMP, timestamp);
      String batchTimestampFinish = putHeader(events.get(events.size() - 1), HEADER_TIMESTAMP, timestamp);
      for (int i = 0; i < events.size(); i++) {
        setEventHeaders(events.get(i), timestamp, batchId, i + 1, events.size(), batchTimestampStart, batchTimestampFinish);
      }
      return events;
    }

    @Override
    public void close() {
    }

    private Event setEventHeaders(Event event, String timestamp, String batchId, int batchIndex, int batchSum, String batchTimestampStart,
                                  String batchTimestampFinish) {
      putHeader(event, HEADER_TIMESTAMP, "" + timestamp);
      putHeader(event, HEADER_BATCH_TYPE, recordType);
      putHeader(event, HEADER_BATCH_ID, batchId, true);
      putHeader(event, HEADER_BATCH_INDEX, "" + batchIndex, true);
      putHeader(event, HEADER_BATCH_SUM, "" + batchSum, true);
      putHeader(event, HEADER_BATCH_TIMESTAMP_START, "" + batchTimestampStart, true);
      putHeader(event, HEADER_BATCH_TIMESTAMP_FINISH, "" + batchTimestampFinish, true);
      return event;
    }

    public static class Builder implements org.apache.flume.interceptor.Interceptor.Builder {

      private String recordType;

      @Override
      public Interceptor build() {
        return new Interceptor(recordType);
      }

      @Override
      public void configure(Context context) {
        try {
          recordType = context.getString(PROPERTY_RECORD_TYPE, RECORD_TYPE_DEFAULT);
        } catch (IllegalArgumentException exception) {
          throw new IllegalArgumentException("Interceptor [" + this.getClass().getName() + "] has illegal paramater ["
            + PROPERTY_RECORD_TYPE + "] value [" + recordType + "]");
        }
      }

    }

  }

  public static class Serializer implements SequenceFileSerializer {

    private static final String DFS_CODEC = "none";
    private static final String DFS_CONTAINER = "sequence";
    private static final String DFS_BATCH_PATTERN = DFS_CONTAINER + "/%{bt}/" + DFS_CODEC + "/%{btss}_%{btsf}_mydataset-%{bid}.seq";

    @Override
    public Class<RecordKey> getKeyClass() {
      return RecordKey.class;
    }

    @Override
    public Class<Text> getValueClass() {
      return Text.class;
    }

    @Override
    public Iterable<Record> serialize(Event event) {
      RecordKey key = new RecordKey();
      Text value = new Text();
      try {
        value.set(new String(event.getBody(), Charsets.UTF_8.name()));
        key.setHash(event.getHeaders().get(HEADER_TIMESTAMP).hashCode());
        key.setType(event.getHeaders().get(HEADER_BATCH_TYPE));
        key.setCodec(DFS_CODEC);
        key.setContainer(DFS_CONTAINER);
        key.setBatch(new StrSubstitutor(event.getHeaders(), "%{", "}").replace(DFS_BATCH_PATTERN));
        key.setTimestamp(Long.parseLong(event.getHeaders().get(HEADER_TIMESTAMP)));
        key.setValid(true);
      } catch (Exception exception) {
        key.setValid(false);
      }
      return Collections.singletonList(new Record(key, value));
    }

    public static class Builder implements SequenceFileSerializer.Builder {

      @Override
      public SequenceFileSerializer build(Context context) {
        return new Serializer();
      }

    }

  }

}
