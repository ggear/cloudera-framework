package com.cloudera.framework.testing.server;

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
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.sun.jersey.api.model.Parameter.Source;

/**
 * Flume {@link TestRule}
 */
public class FlumeServer extends CdhServer<FlumeServer, FlumeServer.Runtime> {

  public enum Runtime {
    LOCAL_CRANKED
  };

  public static synchronized FlumeServer getInstance() {
    return getInstance(instance == null ? Runtime.LOCAL_CRANKED : instance.getRuntime());
  }

  public static synchronized FlumeServer getInstance(Runtime runtime) {
    return instance == null ? instance = new FlumeServer(runtime) : instance.assertRuntime(runtime);
  }

  /**
   * Crank a single Flume <code>sink</code> and <code>source</code> pipeline,
   * wired together automatically, used to test custom {@link Source sources} or
   * {@link Sink sinks}. A full Flume deployment is suggested to integration
   * test a full Flume pipeline configuration.
   *
   * @param substitutions
   *          Config file $KEY, VALUE substitutions
   * @param configFile
   *          optional (can be <code>null</code>) classpath relative Flume
   *          config, including <code>agentName</code>, <code>sourceName</code>
   *          and <code>sinkName</code> prefixed configuration properties
   * @param configSourceOverlay
   *          configuration properties overlay for the source, not prefixed with
   *          <code>agentName</code> or <code>sourceName</code>
   * @param configSinkOverlay
   *          configuration properties overlay for the sink, not prefixed with
   *          <code>agentName</code> or <code>sinkName</code>
   * @param agentName
   *          the agent name
   * @param sourceName
   *          the source name
   * @param sinkName
   *          the sink name
   * @param source
   *          the source instance
   * @param sink
   *          the sink instance
   * @param outputPath
   *          the optional (can be <code>null</code>) root DFS output path for
   *          the sink
   * @param iterations
   *          the number of iterations to run the source and sink for
   * @return the number of files existing post-process under
   *         <code>outputPath</code>, 0 if <code>outputPath</code> is
   *         <code>null</code>
   */
  public synchronized int crankPipeline(Map<String, String> substitutions, String configFile,
      Map<String, String> configSourceOverlay, Map<String, String> configSinkOverlay, String agentName,
      String sourceName, String sinkName, PollableSource source, Sink sink, String outputPath, int iterations)
      throws IOException, EventDeliveryException, InterruptedException {
    Properties config = new Properties();
    if (configFile != null) {
      InputStream configStream = FlumeServer.class.getClassLoader().getResourceAsStream(configFile);
      if (configStream != null) {
        config.load(configStream);
        configStream.close();
      }
    }
    String sinkConfigPrefix = agentName + ".sinks." + sinkName + ".";
    String sourceConfigPrefix = agentName + ".sources." + sourceName + ".";
    Map<String, String> sinkConfig = new HashMap<>();
    Map<String, String> sourceConfig = new HashMap<>();
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
    if (outputPath != null) {
      DfsServer.getInstance().getFileSystem().mkdirs(DfsServer.getInstance().getPath(outputPath));
    }
    Channel channel = new MemoryChannel();
    channel.setName(sourceName + "-" + sinkName + "-channel");
    Configurables.configure(channel, new Context(ImmutableMap.of("keep-alive", "1")));
    channel.start();
    sink.setName(sinkName);
    Configurables.configure(sink, new Context(sinkConfig));
    sink.setChannel(channel);
    sink.start();
    List<Channel> channels = new ArrayList<>(1);
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
    }
    for (int i = 0; i < iterations; i++) {
      sink.process();
    }
    source.stop();
    sink.stop();
    channel.stop();
    return outputPath == null ? 0 : DfsServer.getInstance().listFilesDfs(outputPath).length;
  }

  @Override
  public int getIndex() {
    return 70;
  }

  @Override
  public CdhServer<?, ?>[] getDependencies() {
    return new CdhServer<?, ?>[] { DfsServer.getInstance() };
  }

  @Override
  public synchronized void start() throws Exception {
    long time = log(LOG, "start");
    log(LOG, "start", time);
  }

  @Override
  public synchronized void stop() throws IOException {
    long time = log(LOG, "stop");
    log(LOG, "stop", time);
  }

  private static final Logger LOG = LoggerFactory.getLogger(FlumeServer.class);

  private static FlumeServer instance;

  private FlumeServer(Runtime runtime) {
    super(runtime);
  }

}
