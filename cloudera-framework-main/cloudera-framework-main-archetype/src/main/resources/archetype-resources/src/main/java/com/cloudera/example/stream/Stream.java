package com.cloudera.example.stream;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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

import com.cloudera.example.model.Record;
import com.cloudera.example.model.RecordKey;
import com.cloudera.example.model.RecordType;

/**
 * Flume {@link Source} to generate dataset events
 */
public class Stream extends AbstractSource implements Configurable, PollableSource {

  public static final String HEADER_AGENT_ID = "aid";
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

  private static final int RECORD_SIZE_TYPICAL = 128;
  private static final RecordType RECORD_TYPE_DEFAULT = RecordType.TEXT_CSV;

  private static final Logger LOG = LoggerFactory.getLogger(Stream.class);

  private int pollMs = 1000;
  private int pollTicks = 0;
  private int batchSize = 1;
  private RecordType recordType;
  private int recordNumber = 10;

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
      recordType = RecordType.valueOf(context.getString(PROPERTY_RECORD_TYPE, RECORD_TYPE_DEFAULT.toString()));
    } catch (IllegalArgumentException exception) {
      throw new IllegalArgumentException(
          "Source [" + getName() + "] has illegal paramater [" + PROPERTY_RECORD_TYPE + "] value [" + recordType + "]");
    }
    recordNumber = context.getInteger(PROPERTY_RECORD_NUMBER, recordNumber);
    if (recordNumber < 1) {
      throw new IllegalArgumentException("Source [" + getName() + "] has illegal paramater [" + PROPERTY_RECORD_NUMBER
          + "] value [" + recordNumber + "]");
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
    eventBatch = new ArrayList<Event>();
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
    Map<String, String> header = new HashMap<String, String>();
    header.put(HEADER_AGENT_ID, AGENT_ID);
    header.put(HEADER_BATCH_TYPE, recordType.getQualifier());
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
      StringBuilder record = new StringBuilder(recordNumber * RECORD_SIZE_TYPICAL);
      for (int i = 0; i < recordNumber; i++) {
        record
            .append(recordType.serialise(Record.newBuilder().setMyTimestamp(System.currentTimeMillis())
                .setMyInteger((int) (Math.random() * 10)).setMyDouble(Math.random())
                .setMyBoolean(Math.random() < 0.5 ? true : false).setMyString(UUID.randomUUID().toString()).build()))
            .append("\n");
      }
      processEvent(EventBuilder.withBody(record.toString(), Charset.forName(Charsets.UTF_8.name()),
          getEventHeader(System.currentTimeMillis())), false);
      int sleepMs = 0;
      boolean tickRequired = false;
      int tickMs = pollMs / (pollTicks + 1);
      for (int i = 0; i <= pollTicks; i++) {
        if (pollTicks > 0 && i < pollTicks) {
          if (tickRequired) {
            processEvent(EventBuilder.withBody(record.toString(), Charset.forName(Charsets.UTF_8.name()),
                getEventHeader(System.currentTimeMillis())), false);
          } else {
            tickRequired = true;
          }
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
        LOG.error("Source [" + getName() + "] encountered exception processing event, " + "backing off and retrying",
            exception);
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Source [" + getName() + "] process stopped, ms [" + (System.currentTimeMillis() - time) + "]");
    }

    return status;
  }

  public static class Interceptor implements org.apache.flume.interceptor.Interceptor {

    private static final Logger LOG = LoggerFactory.getLogger(Interceptor.class);

    private RecordType recordType;

    public Interceptor(RecordType recordType) {
      this.recordType = recordType;
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
      return getEventWithHeaders(event, timestamp, UUID.randomUUID().toString(), 1, 1, timestamp, timestamp);
    }

    @Override
    public List<Event> intercept(List<Event> events) {
      String timestamp = "" + System.currentTimeMillis();
      String batchId = UUID.randomUUID().toString();
      String batchTimestampStart = putHeader(events.get(0), HEADER_TIMESTAMP, timestamp);
      String batchTimestampFinish = putHeader(events.get(events.size() - 1), HEADER_TIMESTAMP, timestamp);
      for (int i = 0; i < events.size(); i++) {
        getEventWithHeaders(events.get(i), timestamp, batchId, i + 1, events.size(), batchTimestampStart,
            batchTimestampFinish);
      }
      return events;
    }

    @Override
    public void close() {
    }

    private Event getEventWithHeaders(Event event, String timestamp, String batchId, int batchIndex, int batchSum,
        String batchTimestampStart, String batchTimestampFinish) {
      putHeader(event, HEADER_AGENT_ID, AGENT_ID);
      putHeader(event, HEADER_TIMESTAMP, "" + timestamp);
      putHeader(event, HEADER_BATCH_ID, batchId);
      putHeader(event, HEADER_BATCH_TYPE, recordType.getQualifier());
      putHeader(event, HEADER_BATCH_INDEX, "" + batchIndex, true);
      putHeader(event, HEADER_BATCH_SUM, "" + batchSum, true);
      putHeader(event, HEADER_BATCH_TIMESTAMP_START, "" + batchTimestampStart, true);
      putHeader(event, HEADER_BATCH_TIMESTAMP_FINISH, "" + batchTimestampFinish, true);
      return event;
    }

    private static String putHeader(Event event, String key, String value) {
      return putHeader(event, key, value, false);
    }

    private static String putHeader(Event event, String key, String value, boolean force) {
      String valuePrevious = event.getHeaders().get(key);
      if (force || valuePrevious == null) {
        event.getHeaders().put(key, value);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding event header [" + key + "] with value [" + value + "]"
              + (valuePrevious == null ? "" : " overwriting previous value [" + valuePrevious + "]"));
        }
      }
      return force || valuePrevious == null ? value : valuePrevious;
    }

    public static class Builder implements org.apache.flume.interceptor.Interceptor.Builder {

      private RecordType recordType;

      @Override
      public void configure(Context context) {
        try {
          recordType = RecordType.valueOf(context.getString(PROPERTY_RECORD_TYPE, RECORD_TYPE_DEFAULT.toString()));
        } catch (IllegalArgumentException exception) {
          throw new IllegalArgumentException("Interceptor [" + this.getClass().getName() + "] has illegal paramater ["
              + PROPERTY_RECORD_TYPE + "] value [" + recordType + "]");
        }
      }

      @Override
      public Interceptor build() {
        return new Interceptor(recordType);
      }

    }

  }

  public static class Serializer implements SequenceFileSerializer {

    private static final String DFS_BATCH_PATTERN = "sequence/%{type}/none/%{batch}_mydataset-%{agent}.seq";

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
        key.setHash(event.getHeaders().get(HEADER_TIMESTAMP).hashCode());
        key.setTimestamp(Long.parseLong(event.getHeaders().get(HEADER_TIMESTAMP)));
        key.setBatch(new StrSubstitutor(event.getHeaders(), "%{", "}").replace(DFS_BATCH_PATTERN));
        value.set(new String(event.getBody(), Charsets.UTF_8.name()));
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
