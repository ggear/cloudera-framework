package com.cloudera.framework.main.test;

import java.io.IOException;
import java.util.Collections;

import org.apache.flume.EventDeliveryException;
import org.apache.flume.sink.hdfs.HDFSEventSink;
import org.apache.flume.source.SequenceGeneratorSource;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

/**
 * LocalClusterDfsMrFlumeTest system test
 */
public class LocalClusterDfsMrFlumeTestTest extends LocalClusterDfsMrFlumeTest {

  @Test
  public void testFlume() throws IOException, EventDeliveryException, InterruptedException {
    Assert.assertEquals(1,
        processSouceSinkPipeline(ImmutableMap.of("HDFS_ROOT", getPathString("/")), "flume/flume-conf.properties",
            Collections.<String, String> emptyMap(), Collections.<String, String> emptyMap(), "agent1", "source1",
            "sink1", new SequenceGeneratorSource(), new HDFSEventSink(), "/tmp/flume-sink1", 1));
    Assert.assertTrue(processSouceSinkPipeline(ImmutableMap.of("HDFS_ROOT", getPathString("/")),
        "flume/flume-conf.properties", ImmutableMap.of("batchSize", "3"), Collections.<String, String> emptyMap(),
        "agent1", "source1", "sink2", new SequenceGeneratorSource(), new HDFSEventSink(), "/tmp/flume-sink2", 5) >= 1);
  }

}
