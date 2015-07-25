package com.cloudera.example.stream;

import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.sink.hdfs.HDFSEventSink;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import com.cloudera.example.TestConstants;
import com.cloudera.example.model.RecordType;
import com.cloudera.example.stream.Stream.Interceptor;
import com.cloudera.framework.main.test.LocalClusterDfsMrTest;
import com.google.common.collect.ImmutableMap;

public class StreamTestOld extends LocalClusterDfsMrTest implements TestConstants {

  @Test
  public void testStreamSourceSingle() throws Exception {
    Assert.assertEquals(1, processStreamSource("50", "0", "1", 1, true, null));
    Assert.assertEquals(3, processStreamSource("50", "0", "1", 3, true, null));
  }

  @Test
  public void testStreamSourceBatch() throws Exception {
    Assert.assertEquals(3, processStreamSource("50", "0", "3", 3, true, null));
    Assert.assertEquals(3, processStreamSource("50", "1", "1", 3, true, null));
    Assert.assertEquals(3, processStreamSource("50", "1", "3", 3, true, null));
    Assert.assertEquals(27, processStreamSource("50", "9", "1", 3, true, null));
    Assert.assertEquals(25, processStreamSource("50", "9", "5", 3, true, null));
  }

  @Test
  public void testStreamSourceBatchPoll() throws Exception {
    Assert.assertEquals(3, processStreamSource("50", "0", "1", 3, true, null));
    Assert.assertEquals(3, processStreamSource("50", "0", "3", 3, true, null));
    Assert.assertEquals(3, processStreamSource("50", "1", "1", 3, true, null));
    Assert.assertEquals(3, processStreamSource("50", "1", "3", 3, true, null));
    Assert.assertEquals(27, processStreamSource("50", "9", "1", 3, true, null));
    Assert.assertEquals(27, processStreamSource("50", "9", "3", 3, true, null));
  }

  @Test
  public void testHdfsSinkSingle() throws Exception {
    Assert.assertEquals(3, processHdfsSink(3, 1));
  }

  @Test
  public void testHdfsSinkBatch() throws Exception {
    Assert.assertEquals(9, processHdfsSink(3, 4));
  }

  private int processStreamSource(String pollMs, String pollTicks, String batchSize, int iterations, boolean validate,
      Channel channel) throws Exception {
    int eventCount = 0;
    if (validate) {
      channel = new MemoryChannel();
      channel.setName("simple-memory-channel");
      Configurables.configure(channel, new Context(ImmutableMap.of("keep-alive", "1")));
      channel.start();
    }
    List<Channel> channels = new ArrayList<Channel>(1);
    channels.add(channel);
    ChannelSelector channelSelector = new ReplicatingChannelSelector();
    channelSelector.setChannels(channels);
    try {
      Context context = new Context();
      context.put(Stream.PROPERTY_POLL_MS, pollMs);
      context.put(Stream.PROPERTY_POLL_TICKS, pollTicks);
      context.put(Stream.PROPERTY_BATCH_SIZE, batchSize);
      Stream source = new Stream();
      source.setName("simple-stream-source");
      source.configure(context);
      ChannelProcessor channelProcessor = new ChannelProcessor(channelSelector);
      context.put("interceptors", "simple-steam-interceptor");
      context.put("interceptors.simple-steam-interceptor.type", Interceptor.Builder.class.getName());
      channelProcessor.configure(context);
      source.setChannelProcessor(channelProcessor);
      source.start();
      long transactionTimestamp = System.currentTimeMillis() / 1000;
      for (int i = 0; i < iterations; i++) {
        context.put(Stream.PROPERTY_RECORD_TYPE, RecordType.TEXT_COMMA.toString());
        Configurables.configure(source, context);
        source.process();
        if (validate) {
          Transaction transaction = channel.getTransaction();
          transaction.begin();
          Event event = null;
          while ((event = channel.take()) != null) {
            if (event != null) {
              Assert.assertNotNull(event.getHeaders().get(Stream.HEADER_TIMESTAMP));
              Assert.assertTrue(
                  Long.parseLong(event.getHeaders().get(Stream.HEADER_TIMESTAMP)) <= System.currentTimeMillis());
              Assert
                  .assertTrue(Long.parseLong(event.getHeaders().get(Stream.HEADER_TIMESTAMP)) >= transactionTimestamp);
              Assert.assertNotNull(event.getBody());
              Assert.assertTrue(event.getBody().length > 0);
              eventCount++;
            }
          }
          transaction.commit();
          transaction.close();
        }
      }
      source.stop();
    } catch (Exception exception) {
      exception.printStackTrace();
      eventCount = -1;
    }
    if (validate) {
      channel.stop();
    }
    return eventCount;
  }

  private int processHdfsSink(int batchCount, int batchSize) throws Exception {
    String pathLanded = getPathDfs(DIR_DS_MYDATASET_RAW_SOURCE_TEXT_COMMA);
    getFileSystem().mkdirs(new Path(pathLanded));
    Channel channel = new MemoryChannel();
    channel.setName("simple-memory-channel");
    Configurables.configure(channel, new Context(ImmutableMap.of("keep-alive", "1")));
    channel.start();
    Context context = new Context();
    context.put("hdfs.path", pathLanded + "/%{" + Stream.HEADER_BATCH + "}_mydataset-%{" + Stream.HEADER_AGENT_ID
        + "}%{" + Stream.HEADER_TYPE + "}");
    context.put("hdfs.filePrefix", "%{" + Stream.HEADER_TIMESTAMP + "}_mydataset-%{" + Stream.HEADER_INDEX + "}-of-%{"
        + Stream.HEADER_TOTAL + "}");
    context.put("hdfs.inUsePrefix", "_");
    context.put("hdfs.inUseSuffix", "");
    context.put("hdfs.rollCount", "1");
    context.put("hdfs.batchSize", "" + batchSize);
    context.put("hdfs.writeFormat", "Text");
    context.put("hdfs.useRawLocalFileSystem", Boolean.toString(true));
    context.put("hdfs.fileType", "DataStream");
    HDFSEventSink sink = new HDFSEventSink();
    sink.setName("simple-hdfs-sink");
    sink.configure(context);
    sink.setChannel(channel);
    sink.start();
    for (int i = 0; i < batchCount; i++) {
      processStreamSource("50", "" + (batchSize - 1), "" + batchSize, 1, false, channel);
      sink.process();
    }
    sink.stop();
    channel.stop();
    return listFilesDfs(DIR_DS_MYDATASET_RAW_SOURCE_TEXT_COMMA).length;
  }

}
