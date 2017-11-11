package com.cloudera.framework.testing.server;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.cloudera.framework.assembly.ScriptUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.package$;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark {@link TestRule}
 */
public class SparkServer extends CdhServer<SparkServer, SparkServer.Runtime> {

  public static final String SPARK_CONF_MASTER = "spark.master";
  public static final String SPARK_CONF_APPNAME = "spark.app.name";
  public static final String SPARK_CONF_WAREHOUSE = "spark.sql.warehouse.dir";
  public static final String SPARK_CONF_SQLCODEGEN = "spark.sql.codegen.wholeStage";

  public static final List<String> SPARK_ENV = Arrays.asList(new String[]{"SPARK_HOME", "SPARK_CONF_DIR"});

  private static final Logger LOG = LoggerFactory.getLogger(SparkServer.class);

  private static SparkServer instance;

  private SparkServer(Runtime runtime) {
    super(runtime);
  }

  /**
   * Get instance with default runtime
   */
  public static synchronized SparkServer getInstance() {
    return getInstance(instance == null ? Runtime.LOCAL_CONTEXT : instance.getRuntime());
  }

  /**
   * Get instance with specific <code>runtime</code>
   */
  public static synchronized SparkServer getInstance(Runtime runtime) {
    return instance == null ? instance = new SparkServer(runtime) : instance.assertRuntime(runtime);
  }

  @Override
  public int getIndex() {
    return 50;
  }

  @Override
  public CdhServer<?, ?>[] getDependencies() {
    return new CdhServer<?, ?>[]{DfsServer.getInstance()};
  }

  @Override
  public synchronized boolean isValid() {
    if (package$.MODULE$.SPARK_VERSION().charAt(0) == '1') {
      if (!envScalaVersion.equals("2.10")) {
        log(LOG, "error", "Scala 2.10 required, " + envScalaVersion + " detected");
        return false;
      }
    } else if (package$.MODULE$.SPARK_VERSION().charAt(0) == '2') {
      if (!envScalaVersion.equals("2.11")) {
        log(LOG, "error", "Scala 2.11 required, " + envScalaVersion + " detected");
        return false;
      }
    }
    return true;
  }

  @Override
  public synchronized void start() throws Exception {
    long time = log(LOG, "start");
    CdhServer.setEnvProperty("SPARK_HOME", null);
    CdhServer.setEnvProperty("SPARK_CONF_DIR", null);
    System.setProperty(SPARK_CONF_APPNAME, "spark-unit-test");
    System.setProperty(SPARK_CONF_MASTER, "local[*]");
    System.setProperty(SPARK_CONF_WAREHOUSE, new Path(DfsServer.getInstance().getPathUri("/usr/spark/warehouse")).toString());
    System.setProperty(SPARK_CONF_SQLCODEGEN, "false");
    new JavaSparkContext(new SparkConf()).close();
    System.setProperty(ScriptUtil.PropertySparkMaster(), System.getProperty(SPARK_CONF_MASTER));
    log(LOG, "start", time);
  }

  @Override
  public synchronized void stop() throws IOException {
    long time = log(LOG, "stop");
    System.clearProperty(SPARK_CONF_APPNAME);
    System.clearProperty(SPARK_CONF_MASTER);
    System.clearProperty(SPARK_CONF_WAREHOUSE);
    System.clearProperty(ScriptUtil.PropertySparkMaster());
    log(LOG, "stop", time);
  }

  @Override
  protected String logPrefix() {
    return (package$.MODULE$.SPARK_VERSION().charAt(0) == '1' ? "SparkServer" : "Spark2Server") + "." + (getRuntime() == null ? "DEFAULT" :
      getRuntime());
  }

  public enum Runtime {
    LOCAL_CONTEXT // Local Spark context, multi-thread, light-weight
  }
}
