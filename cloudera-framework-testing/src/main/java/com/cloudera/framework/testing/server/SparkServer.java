package com.cloudera.framework.testing.server;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark {@link TestRule}
 */
public class SparkServer extends CdhServer<SparkServer, SparkServer.Runtime> {

  public enum Runtime {
    LOCAL_CONTEXT
  };

  public static synchronized SparkServer getInstance() {
    return getInstance(instance == null ? Runtime.LOCAL_CONTEXT : instance.getRuntime());
  }

  public static synchronized SparkServer getInstance(Runtime runtime) {
    return instance == null ? instance = new SparkServer(runtime) : instance.assertRuntime(runtime);
  }

  public synchronized JavaSparkContext getContext() {
    return context;
  }

  @Override
  public int getIndex() {
    return 50;
  }

  @Override
  public CdhServer<?, ?>[] getDependencies() {
    return new CdhServer<?, ?>[] { DfsServer.getInstance() };
  }

  @Override
  public synchronized void start() throws Exception {
    long time = log(LOG, "start");
    context = new JavaSparkContext("local", "unit-test", new SparkConf().setAppName("Spark Unit-Test"));
    log(LOG, "start", time);
  }

  @Override
  public synchronized void stop() throws IOException {
    long time = log(LOG, "stop");
    if (context != null) {
      context.close();
    }
    log(LOG, "stop", time);
  }

  private static final Logger LOG = LoggerFactory.getLogger(SparkServer.class);

  private static SparkServer instance;

  private JavaSparkContext context;

  private SparkServer(Runtime runtime) {
    super(runtime);
  }

}
