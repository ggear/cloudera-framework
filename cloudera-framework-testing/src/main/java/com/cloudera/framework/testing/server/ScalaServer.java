package com.cloudera.framework.testing.server;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.cloudera.framework.assembly.ScriptUtil;
import com.jag.maven.templater.TemplaterUtil;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

/**
 * Python {@link TestRule}
 */
public class ScalaServer extends CdhServer<ScalaServer, ScalaServer.Runtime> {

  private static final Logger LOG = LoggerFactory.getLogger(ScalaServer.class);

  private static ScalaServer instance;

  private ScalaServer(Runtime runtime) {
    super(runtime);
  }

  /**
   * Get instance with default runtime
   */
  public static synchronized ScalaServer getInstance() {
    return getInstance(instance == null ? Runtime.LOCAL_SCALA_2_11 : instance.getRuntime());
  }

  /**
   * Get instance with specific <code>runtime</code>
   */
  public static synchronized ScalaServer getInstance(Runtime runtime) {
    return instance == null ? instance = new ScalaServer(runtime) : instance.assertRuntime(runtime);
  }

  /**
   * Execute a <code>file</code>
   *
   * @return the exit code
   */
  public int execute(File file) {
    return execute(file, null);
  }

  /**
   * Execute a <code>file</code> with <code>parameters</code>
   *
   * @return the exit code
   */
  public int execute(File file, List<String> parameters) {
    return execute(file, parameters, null);
  }

  /**
   * Execute a <code>file</code> with <code>parameters</code> and <code>environment</code>
   *
   * @return the exit code
   */
  public int execute(File file, List<String> parameters, Map<String, String> environment) {
    return execute(file, parameters, environment, null);
  }

  /**
   * Execute a <code>file</code> with <code>parameters</code> and <code>environment</code>, writing stdout and stderr to <code>output</code>
   *
   * @return the exit code
   */
  public int execute(File file, List<String> parameters, Map<String, String> environment, StringBuffer output) {
    return execute(file, parameters, environment, output, false);
  }

  /**
   * Execute a <code>file</code> with <code>parameters</code> and <code>environment</code>, writing stdout and stderr to <code>output</code>,
   * suppressing all logging if <code>quiet</code>
   *
   * @return the exit code
   */
  public int execute(File file, List<String> parameters, Map<String, String> environment, StringBuffer output, boolean quiet) {
    if (environment == null) {
      environment = new HashMap<>();
    }
    environment.put("JAVA_OPTS", "-Xmx2g");
    if (ScriptUtil.getHadoopDefaultFs().get() != null) {
      environment.put(ScriptUtil.PropertyHadoopDefaultFs(), ScriptUtil.getHadoopDefaultFs().get());
    }
    if (ScriptUtil.getSparkMaster().get() != null) {
      environment.put(ScriptUtil.PropertySparkMaster(), ScriptUtil.getSparkMaster().get());
    }
    if (output == null) {
      output = new StringBuffer();
    }
    int exit = TemplaterUtil.executeScriptScala(scala.Option.apply(JavaConversions.<String, String>mapAsScalaMap(environment)), file,
      scala.Option.apply(parameters == null ? null : JavaConversions.<String>asScalaBuffer(parameters)),
      new File(REL_DIR_SCRIPT, UUID.randomUUID().toString()), scala.Option.apply(null), scala.Option.apply(output));
    if (!quiet) {
      log(LOG, "execute", "script [" + file.getAbsolutePath() + "]" + output.toString(), true);
    }
    return exit;
  }

  @Override
  public int getIndex() {
    return 110;
  }

  @Override
  public CdhServer<?, ?>[] getDependencies() {
    return new CdhServer<?, ?>[]{DfsServer.getInstance(DfsServer.Runtime.CLUSTER_DFS)};
  }

  @Override
  public synchronized void start() throws Exception {
    long time = log(LOG, "start");
    switch (getRuntime()) {
      case LOCAL_SCALA_2_11:
        break;
      default:
        throw new IllegalArgumentException("Unsupported [" + getClass().getSimpleName() + "] runtime [" + getRuntime() + "]");
    }
    log(LOG, "start", time);
  }

  @Override
  public synchronized void stop() throws IOException {
    long time = log(LOG, "stop");
    switch (getRuntime()) {
      case LOCAL_SCALA_2_11:
        break;
      default:
        throw new IllegalArgumentException("Unsupported [" + getClass().getSimpleName() + "] runtime [" + getRuntime() + "]");
    }
    log(LOG, "stop", time);
  }

  public enum Runtime {
    LOCAL_SCALA_2_11 // Local Scala 2.11 script wrapper, single-process, heavy-weight
  }

}
