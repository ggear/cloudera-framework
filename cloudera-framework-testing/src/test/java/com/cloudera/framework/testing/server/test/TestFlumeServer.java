package com.cloudera.framework.testing.server.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;

import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.FlumeServer;
import com.google.common.collect.ImmutableMap;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.sink.hdfs.HDFSEventSink;
import org.apache.flume.source.SequenceGeneratorSource;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(TestRunner.class)
public class TestFlumeServer implements TestConstants {

  @ClassRule
  public static DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static FlumeServer flumeServer = FlumeServer.getInstance();

  @Test
  public void testFlume() throws InterruptedException, IOException, EventDeliveryException {
    assertEquals(1,
      flumeServer.crankPipeline(ImmutableMap.of("HDFS_ROOT", dfsServer.getPathUri("/")), "flume/flume-conf.properties",
        Collections.<String, String>emptyMap(), Collections.<String, String>emptyMap(), "agent1", "source1", "sink1",
        new SequenceGeneratorSource(), new HDFSEventSink(), "/tmp/flume-sink1", 1));
    assertTrue(flumeServer.crankPipeline(ImmutableMap.of("HDFS_ROOT", dfsServer.getPathUri("/")), "flume/flume-conf.properties",
      ImmutableMap.of("batchSize", "3"), Collections.<String, String>emptyMap(), "agent1", "source1", "sink2",
      new SequenceGeneratorSource(), new HDFSEventSink(), "/tmp/flume-sink2", 5) >= 1);
  }

  @Test
  public void testFlumeAgain() throws InterruptedException, IOException, EventDeliveryException {
    testFlume();
  }

}
