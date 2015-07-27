package com.cloudera.framework.main.test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.Sink;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRConfig;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

/**
 * Base class for all local-cluster DFS, MR and Flume tests, single-process,
 * multi-threaded DFS facade over local file system and local MR job runner,
 * provides fast, functional read/write API compatibility, isolated and
 * idempotent runtime
 */
public class LocalClusterDfsMrFlumeTest extends BaseTest {

  private static Logger LOG = LoggerFactory.getLogger(LocalClusterDfsMrFlumeTest.class);

  private static JobConf conf;
  private static FileSystem fileSystem;

  public LocalClusterDfsMrFlumeTest() {
    super();
  }

  public LocalClusterDfsMrFlumeTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets,
      String[][][] labels, @SuppressWarnings("rawtypes") Map[] metadata) {
    super(sources, destinations, datasets, subsets, labels, metadata);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public FileSystem getFileSystem() {
    return fileSystem;
  }

  @Override
  public String getPathDfs(String path) {
    String pathRelativeToDfsRootSansLeadingSlashes = stripLeadingSlashes(path);
    return pathRelativeToDfsRootSansLeadingSlashes.equals("") ? REL_DIR_DFS_LOCAL
        : new Path(REL_DIR_DFS_LOCAL, pathRelativeToDfsRootSansLeadingSlashes).toUri().toString();
  }

  /**
   * Process a Flume pipeline
   *
   * @param substitutions
   * @param configFile
   * @param configSourceOverlay
   * @param configSinkOverlay
   * @param agentName
   * @param sourceName
   * @param sinkName
   * @param source
   * @param sink
   * @param outputPath
   * @param iterations
   * @return
   * @throws IOException
   * @throws EventDeliveryException
   */
  public int processPipeline(Map<String, String> substitutions, String configFile,
      Map<String, String> configSourceOverlay, Map<String, String> configSinkOverlay, String agentName,
      String sourceName, String sinkName, PollableSource source, Sink sink, String outputPath, int iterations)
          throws IOException, EventDeliveryException {
    InputStream configStream = LocalClusterDfsMrFlumeTest.class.getClassLoader().getResourceAsStream(configFile);
    if (configStream == null) {
      throw new IOException("Could not load [" + configFile + "] from classpath");
    }
    Properties config = new Properties();
    config.load(configStream);
    configStream.close();
    String sinkConfigPrefix = agentName + ".sinks." + sinkName + ".";
    String sourceConfigPrefix = agentName + ".sources." + sourceName + ".";
    Map<String, String> sinkConfig = new HashMap<String, String>();
    Map<String, String> sourceConfig = new HashMap<String, String>();
    for (Object keyObject : config.keySet()) {
      String key = (String) keyObject;
      String value = config.getProperty(key);
      for (String keyReplace : substitutions.keySet()) {
        value = value.replaceAll("\\$" + keyReplace, substitutions.get(keyReplace));
      }
      if (key.startsWith(sinkConfigPrefix) && !key.endsWith(sinkConfigPrefix + "type") && !key.endsWith(".channel")) {
        sinkConfig.put(key.replace(sinkConfigPrefix, ""), value);
      } else if (key.startsWith(sourceConfigPrefix) && !key.endsWith(sourceConfigPrefix + "type")
          && !key.endsWith(".selector.type") && !key.endsWith(".channels")) {
        sourceConfig.put(key.replace(sourceConfigPrefix, ""), value);
      }
    }
    sinkConfig.putAll(configSinkOverlay);
    sourceConfig.putAll(configSourceOverlay);
    getFileSystem().mkdirs(new Path(getPathDfs(outputPath)));
    Channel channel = new MemoryChannel();
    channel.setName(sourceName + "-" + sinkName + "-channel");
    Configurables.configure(channel, new Context(ImmutableMap.of("keep-alive", "1")));
    channel.start();
    sink.setName(sinkName);
    Configurables.configure(sink, new Context(sinkConfig));
    sink.setChannel(channel);
    sink.start();
    List<Channel> channels = new ArrayList<Channel>(1);
    channels.add(channel);
    ChannelSelector channelSelector = new ReplicatingChannelSelector();
    channelSelector.setChannels(channels);
    source.setName(sourceName);
    Configurables.configure(source, new Context(sourceConfig));
    ChannelProcessor channelProcessor = new ChannelProcessor(channelSelector);
    Configurables.configure(channelProcessor, new Context(sourceConfig));
    source.setChannelProcessor(channelProcessor);
    source.start();
    for (int i = 0; i < iterations; i++) {
      source.process();
      sink.process();
    }
    source.stop();
    sink.stop();
    channel.stop();
    return listFilesDfs(outputPath).length;
  }

  @BeforeClass
  public static void setUpRuntime() throws Exception {
    long time = debugMessageHeader(LOG, "setUpRuntime");
    fileSystem = FileSystem.getLocal(conf = new JobConf());
    conf.set(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
    debugMessageFooter(LOG, "setUpRuntime", time);
  }

  @Before
  public void setUpFlume() throws Exception {
    long time = debugMessageHeader(LOG, "setUpFlume");
    debugMessageFooter(LOG, "setUpFlume", time);
  }

  @AfterClass
  public static void tearDownRuntime() throws Exception {
    long time = debugMessageHeader(LOG, "tearDownRuntime");
    if (fileSystem != null) {
      fileSystem.close();
    }
    debugMessageFooter(LOG, "tearDownRuntime", time);
  }

}
