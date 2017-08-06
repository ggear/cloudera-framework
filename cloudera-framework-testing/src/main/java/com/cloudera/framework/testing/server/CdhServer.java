package com.cloudera.framework.testing.server;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.parcel.library.ParcelUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.Properties;

/**
 * Base class for all {@link ClassRule} and {@link org.junit.Rule} annotated
 * {@link TestRule TestRules}.
 *
 * @param <U> Specialised class
 * @param <V> Specialised class runtime enum
 */
public abstract class CdhServer<U extends CdhServer<?, ?>, V> extends ExternalResource
  implements TestConstants, Comparable<CdhServer<CdhServer<?, ?>, V>> {

  public static final String SERVER_BIND_IP = "127.0.0.1";
  public static final AtomicInteger SERVER_BIND_PORT_START = new AtomicInteger(25000);
  public static final int SERVER_BIND_PORT_FINISH = 25100;
  private static final Logger LOG = LoggerFactory.getLogger(DfsServer.class);
  private static final Pattern REGEX_SCALA_VERSION = Pattern.compile(".*([1-9]+\\.[0-9]+)\\.[1-9]+.*");
  protected String envOsName;
  protected String envOsDescriptor;
  protected String envScalaVersion;
  private V runtime;
  private int semaphore;
  private Configuration conf;
  private Boolean isValid;


  protected CdhServer(V runtime) {
    conf = new JobConf();
    this.runtime = runtime;
    this.envOsName = System.getProperty("os.name");
    if (this.envOsName == null) {
      throw new RuntimeException("Could not determine host OS");
    }
    this.envOsDescriptor = ParcelUtil.getOsDescriptor();
    Matcher scalaVersionMatcher = REGEX_SCALA_VERSION.matcher(Properties.versionString());
    if (scalaVersionMatcher.find()) {
      this.envScalaVersion = scalaVersionMatcher.group(1);
    } else {
      throw new RuntimeException("Could not determine Scala version");
    }
  }

  /**
   * Get the next available port
   */
  public static int getNextAvailablePort() {
    while (SERVER_BIND_PORT_START.get() < SERVER_BIND_PORT_FINISH) {
      try {
        ServerSocket server = new ServerSocket(SERVER_BIND_PORT_START.getAndIncrement());
        server.close();
        return server.getLocalPort();
      } catch (IOException exception) {
        // ignore
      }
    }
    throw new RuntimeException("Could not find available port");
  }

  /**
   * Set an environment variable
   */
  protected static void setEnvProperty(String key, String value) {
    try {
      Class[] classes = Collections.class.getDeclaredClasses();
      Map<String, String> env = System.getenv();
      for (Class clazz : classes) {
        if ("java.util.Collections$UnmodifiableMap".equals(clazz.getName())) {
          Field field = clazz.getDeclaredField("m");
          field.setAccessible(true);
          Object object = field.get(env);
          Map<String, String> map = (Map<String, String>) object;
          if (value == null) {
            map.remove(key);
          } else {
            map.put(key, value);
          }
        }
      }
    } catch (Exception exception) {
      throw new RuntimeException("Could not set environment property [" + key + "] to [" + value + "]");
    }
  }

  /**
   * Define the index that defines <code>this</code> objects order within the
   * dependency pipeline, the lower the index the earlier in the dependency tree
   * <code>this</code> object will be
   */
  public abstract int getIndex();

  /**
   * Get the list of {@link CdhServer} dependencies, override if there are
   * dependencies
   */
  public CdhServer<?, ?>[] getDependencies() {
    return new CdhServer<?, ?>[0];
  }

  /**
   * Determine whether the runtime environment presented to this service is valid
   */
  public synchronized boolean isValid() {
    return isValid == null ? isValid = testValidity() : isValid;
  }

  /**
   * Determine whether the runtime environment presented to this service is valid
   */
  public synchronized boolean testValidity() {
    return true;
  }

  /**
   * Get the {@link Configuration} associated with this {@link CdhServer}
   */
  public synchronized Configuration getConf() {
    return conf;
  }

  /**
   * Set the {@link Configuration} associated with this {@link CdhServer}
   */
  protected synchronized void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Test to see if this {@link CdhServer} is started
   */
  public synchronized boolean isStarted() {
    return semaphore > 0;
  }

  /**
   * Start the {@link CdhServer}
   */
  public abstract void start() throws Exception;

  /**
   * Clean-up all state persisted by this {@link CdhServer}
   */
  public void clean() throws Exception {
  }

  /**
   * Report on the state persisted by this {@link CdhServer}
   */
  public void state() throws Exception {
  }

  /**
   * Stop the {@link CdhServer}
   */
  public abstract void stop() throws Exception;

  /**
   * Get the configured runtime
   */
  public synchronized V getRuntime() {
    return runtime;
  }

  @SuppressWarnings("NullableProblems")
  @Override
  public int compareTo(CdhServer<CdhServer<?, ?>, V> that) {
    return Integer.compare(that.getIndex(), getIndex());
  }

  @SuppressWarnings("unchecked")
  protected U assertRuntime(V runtime) {
    if (isStarted() && !this.runtime.equals(runtime)) {
      throw new IllegalArgumentException("A server pipeline runtime dependency inconsistency has been detected, please decorate all ["
        + this.getClass().getSimpleName() + "] server instances with [" + runtime
        + "] runtime, explicitly if this server has been created implicitly by a dependent service");
    }
    this.runtime = runtime;
    return (U) this;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    assertTestRunner(description.getClassName());
    return super.apply(base, description);
  }

  @Override
  protected synchronized void before() throws Exception {
    if (semaphore++ == 0) {
      for (CdhServer<?, ?> dependency : getDependencies()) {
        dependency.before();
      }
      if (isValid()) {
        start();
      }
    }
  }

  @Override
  protected synchronized void after() {
    if (--semaphore == 0) {
      try {
        if (isValid()) {
          stop();
        }
        for (CdhServer<?, ?> dependency : getDependencies()) {
          dependency.after();
        }
      } catch (Exception exception) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Unexpected exception stopping server [" + this.getClass().getSimpleName() + "]", exception);
        }
      }
    }
  }

  @SuppressWarnings({"rawtypes", "LoopStatementThatDoesntLoop"})
  private boolean assertTestRunner(String testClass) {
    try {
      RunWith runWith = Class.forName(testClass).getAnnotation(RunWith.class);
      if (runWith == null) {
        throw new RuntimeException("Missing [@" + RunWith.class.getCanonicalName() + "(" + TestRunner.class.getCanonicalName()
          + ".class)] on class [" + testClass + "]");
      }
      if (runWith.value().equals(Suite.class)) {
        SuiteClasses suiteClasses = Class.forName(testClass).getAnnotation(SuiteClasses.class);
        for (Class suiteTestClass : suiteClasses.value()) {
          return assertTestRunner(suiteTestClass.getCanonicalName());
        }
      } else if (!runWith.value().equals(TestRunner.class)) {
        throw new RuntimeException("Unsupported run with [" + runWith.value().getCanonicalName() + "] on class [" + testClass + "]");
      }
    } catch (Exception exception) {
      String message = "The test [" + testClass + "] included a rule [" + getClass().getCanonicalName() + "] but did not include a [@"
        + RunWith.class.getCanonicalName() + "(" + TestRunner.class.getCanonicalName() + ".class)] class annotation";
      if (LOG.isErrorEnabled()) {
        LOG.error(message, exception);
      }
      throw new RuntimeException(message, exception);
    }
    return true;
  }

  protected String logPrefix() {
    return getClass().getSimpleName() + "." + (getRuntime() == null ? "DEFAULT" : getRuntime());
  }

  protected void log(Logger log, String method, String message) {
    log(log, method, message, false);
  }

  protected void log(Logger log, String method, String message, boolean debug) {
    logDetail(log, debug, logPrefix() + " [" + method + "] " + message);
  }

  protected long log(Logger log, String method) {
    return log(log, method, false);
  }

  protected long log(Logger log, String method, boolean debug) {
    logDetail(log, debug, logPrefix() + " [" + method + "] starting ... ");
    return System.currentTimeMillis();
  }

  protected void log(Logger log, String method, long start) {
    log(log, method, start, false);
  }

  protected void log(Logger log, String method, long start, boolean debug) {
    logDetail(log, debug, logPrefix() + " [" + method + "] finished in [" + (System.currentTimeMillis() - start) + "] ms");
  }

  private void logDetail(Logger log, boolean debug, String detail) {
    if (debug) {
      if (log.isDebugEnabled()) {
        log.debug(detail);
      }
    } else if (log.isInfoEnabled()) {
      log.info(detail);
    }
  }

}
