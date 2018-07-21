package com.cloudera.framework.testing.server;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.cloudera.labs.envelope.spark.Contexts;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.spark.package$;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scala {@link TestRule}
 */
public class EnvelopeServer extends CdhServer<EnvelopeServer, EnvelopeServer.Runtime> {

  private static final Logger LOG = LoggerFactory.getLogger(EnvelopeServer.class);

  private static final String DIR_HOME = "usr/hive";
  private static final String DIR_WAREHOUSE = "warehouse";

  private static final AtomicLong DERBY_DB_COUNTER = new AtomicLong();

  private static EnvelopeServer instance;

  private EnvelopeServer(Runtime runtime) {
    super(runtime);
  }

  /**
   * Get instance with default runtime
   */
  public static synchronized EnvelopeServer getInstance() {
    return getInstance(instance == null ? Runtime.LOCAL_CONTEXT : instance.getRuntime());
  }

  /**
   * Get instance with specific <code>runtime</code>
   */
  public static synchronized EnvelopeServer getInstance(Runtime runtime) {
    return instance == null ? instance = new EnvelopeServer(runtime) : instance.assertRuntime(runtime);
  }

  @Override
  public int getIndex() {
    return 130;
  }

  @Override
  public CdhServer<?, ?>[] getDependencies() {
    return new CdhServer<?, ?>[]{DfsServer.getInstance(), SparkServer.getInstance(SparkServer.Runtime.LOCAL_CONTEXT)};
  }

  @Override
  public synchronized boolean isValid() {
    if (package$.MODULE$.SPARK_VERSION().charAt(0) != '2' && !envScalaVersion.equals("2.11")) {
      log(LOG, "error", "Spark 2 and Scala 2.11 required, Spark [1] and Scala [" + envScalaVersion + "] detected");
      return false;
    }
    return true;
  }

  @Override
  public synchronized void start() throws Exception {
    long time = log(LOG, "start");
    Path hiveHomePath = new Path(DfsServer.getInstance().getPathUri("/"), DIR_HOME);
    Path hiveWarehousePath = new Path(hiveHomePath, DIR_WAREHOUSE);
    File derbyDir = new File(ABS_DIR_DERBY_DB);
    String hiveDerbyConnectString = "jdbc:derby:" + derbyDir.getAbsolutePath() + "/test-hive-metastore-"
      + DERBY_DB_COUNTER.incrementAndGet() + ";create=true";
    FileUtils.deleteDirectory(derbyDir);
    derbyDir.mkdirs();
    DfsServer.getInstance().getFileSystem().mkdirs(hiveHomePath);
    DfsServer.getInstance().getFileSystem().mkdirs(hiveWarehousePath);
    FileSystem.mkdirs(DfsServer.getInstance().getFileSystem(), hiveWarehousePath, new FsPermission((short) 511));
    Map<String, Object> parameters = new HashMap<>();
    parameters.put(Contexts.SPARK_SESSION_ENABLE_HIVE_SUPPORT, true);
    parameters.put(Contexts.SPARK_CONF_PROPERTY_PREFIX + "hive.metastore.warehouse.dir", hiveWarehousePath.toString());
    parameters.put(Contexts.SPARK_CONF_PROPERTY_PREFIX + "javax.jdo.option.ConnectionURL", hiveDerbyConnectString);
    Contexts.initialize(ConfigFactory.parseMap(parameters), Contexts.ExecutionMode.BATCH);
    log(LOG, "start", time);
  }

  @Override
  public synchronized void state() throws IllegalArgumentException, IOException {
    long time = log(LOG, "state", true);
    Contexts.closeSparkSession(true);
    Contexts.closeJavaStreamingContext(true);
    log(LOG, "state", time, true);
  }

  @Override
  public synchronized void stop() throws IOException {
    long time = log(LOG, "stop");
    log(LOG, "stop", time);
  }

  public enum Runtime {
    LOCAL_CONTEXT // Local Envelope pipeline wrapper, single-process, heavy-weight
  }

}
