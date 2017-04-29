package com.cloudera.framework.common.flume;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.io.IOUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Source;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Use this {@link Interceptor} to unwrap a Flume encoded event from a direct
 * {@link Source source} {@link Channel channel} (ie Kafka).
 */
public class UnwrapInterceptor implements Interceptor {

  private final static Logger LOG = LoggerFactory.getLogger(UnwrapInterceptor.class);

  private BinaryDecoder decoder = null;
  private SpecificDatumReader<AvroFlumeEvent> reader = null;

  private static Map<String, String> toStringMap(Map<CharSequence, CharSequence> charSequenceMap, Map<String, String> mergeMap) {
    Map<String, String> stringMap = new HashMap<>();
    for (Map.Entry<CharSequence, CharSequence> entry : charSequenceMap.entrySet()) {
      stringMap.put(entry.getKey().toString(), entry.getValue().toString());
    }
    stringMap.putAll(mergeMap);
    return stringMap;
  }

  @Override
  public void initialize() {
    decoder = DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(new byte[0]), decoder);
    reader = new SpecificDatumReader<>(AvroFlumeEvent.class);
    if (LOG.isInfoEnabled()) {
      LOG.info("Event Unwrap Interceptor initialised");
    }
  }

  @Override
  public Event intercept(Event event) {
    return unwrap(event);
  }

  @Override
  public List<Event> intercept(List<Event> events) {
    List<Event> eventsUnwrapped = new ArrayList<>();
    for (Event event : events) {
      eventsUnwrapped.add(unwrap(event));
    }
    return eventsUnwrapped;
  }

  @Override
  public void close() {
  }

  public Event unwrap(Event event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Attempting to unwrap event, body [" + event.getBody().length + "] bytes");
    }
    Event eventUnwrapped = event;
    InputStream eventWrappedStream = new ByteArrayInputStream(event.getBody());
    try {
      decoder = DecoderFactory.get().directBinaryDecoder(eventWrappedStream, decoder);
      AvroFlumeEvent eventUnwrappedAvro = reader.read(null, decoder);
      eventUnwrapped = EventBuilder.withBody(eventUnwrappedAvro.getBody().array(),
        toStringMap(eventUnwrappedAvro.getHeaders(), event.getHeaders()));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Event successfully unwrapped, header [" + eventUnwrappedAvro.getHeaders().size() + "] fields, body ["
          + eventUnwrapped.getBody().length + "] bytes");
      }
    } catch (Exception exception) {
      if (LOG.isWarnEnabled()) {
        LOG.warn("Failed to unwrap event, " + "perhaps this source is not connected to a sinkless connector?", exception);
      }
    } finally {
      IOUtils.closeQuietly(eventWrappedStream);
    }
    return eventUnwrapped;
  }

  public static class Builder implements Interceptor.Builder {

    @Override
    public void configure(Context context) {
    }

    @Override
    public Interceptor build() {
      return new UnwrapInterceptor();
    }

  }

}
