package com.cloudera.framework.testing.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicInteger;

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

import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Rule;
import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestRunner;

/**
 * Base class for all {@link ClassRule} and {@link Rule} annotated
 * {@link TestRule TestRules}.
 *
 * @param <U>
 *          Specialised class
 * @param <V>
 *          Specialised class runtime enum
 */
public abstract class CdhServer<U extends CdhServer<?, ?>, V> extends ExternalResource
    implements TestConstants, Comparable<CdhServer<CdhServer<?, ?>, V>> {

  public abstract int getIndex();

  public CdhServer<?, ?>[] getDependencies() {
    return new CdhServer<?, ?>[0];
  }

  public synchronized Configuration getConf() {
    return conf;
  }

  public synchronized boolean isStarted() {
    return semaphore > 0;
  }

  public abstract void start() throws Exception;

  public void clean() throws Exception {
  }

  public void state() throws Exception {
  }

  public abstract void stop() throws Exception;

  public synchronized V getRuntime() {
    return runtime;
  }

  protected synchronized void setConf(Configuration conf) {
    this.conf = conf;
  }

  private static Logger LOG = LoggerFactory.getLogger(DfsServer.class);

  private V runtime;
  private int semaphore;
  private Configuration conf;

  protected CdhServer(V runtime) {
    conf = new JobConf();
    this.runtime = runtime;
  }

  @Override
  protected synchronized void before() throws Exception {
    if (semaphore++ == 0) {
      for (CdhServer<?, ?> dependency : getDependencies()) {
        dependency.before();
      }
      start();
    }
  }

  @Override
  protected synchronized void after() {
    if (--semaphore == 0) {
      try {
        stop();
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

  @Override
  public int compareTo(CdhServer<CdhServer<?, ?>, V> that) {
    return getIndex() < that.getIndex() ? 1 : getIndex() > that.getIndex() ? -1 : 0;
  }

  @SuppressWarnings("unchecked")
  protected U assertRuntime(V runtime) {
    if (isStarted() && !this.runtime.equals(runtime)) {
      throw new IllegalArgumentException(
          "A server pipeline runtime dependency inconsistency has been detected, please decorate all ["
              + this.getClass().getSimpleName() + "] server instances with [" + runtime
              + "] runtime, explicitly if this server has been created implicitly by a depedant service");
    }
    this.runtime = runtime;
    return (U) this;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    assertTestRunner(description.getClassName());
    return super.apply(base, description);
  }

  @SuppressWarnings("rawtypes")
  private boolean assertTestRunner(String testClass) {
    try {
      RunWith runWith = Class.forName(testClass).getAnnotation(RunWith.class);
      if (runWith == null) {
        throw new RuntimeException("Missing [@" + RunWith.class.getCanonicalName() + "("
            + TestRunner.class.getCanonicalName() + ".class)] on class [" + testClass + "]");
      }
      if (runWith.value().equals(Suite.class)) {
        SuiteClasses suiteClasses = Class.forName(testClass).getAnnotation(SuiteClasses.class);
        for (Class suiteTestClass : suiteClasses.value()) {
          return assertTestRunner(suiteTestClass.getCanonicalName());
        }
      } else if (!runWith.value().equals(TestRunner.class)) {
        throw new RuntimeException(
            "Unsupported run with [" + runWith.value().getCanonicalName() + "] on class [" + testClass + "]");
      }
    } catch (Exception exception) {
      String message = "The test [" + testClass + "] included a rule [" + getClass().getCanonicalName()
          + "] but did not include a [@" + RunWith.class.getCanonicalName() + "(" + TestRunner.class.getCanonicalName()
          + ".class)] class annotation";
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
    String detail = logPrefix() + " [" + method + "] " + message;
    if (debug) {
      if (log.isDebugEnabled()) {
        log.debug(detail);
      }
    } else if (log.isInfoEnabled()) {
      log.info(detail);
    }
  }

  protected long log(Logger log, String method) {
    return log(log, method, false);
  }

  protected long log(Logger log, String method, boolean debug) {
    String detail = logPrefix() + " [" + method + "] starting ... ";
    if (debug) {
      if (log.isDebugEnabled()) {
        log.debug(detail);
      }
    } else if (log.isInfoEnabled()) {
      log.info(detail);
    }
    return System.currentTimeMillis();
  }

  protected void log(Logger log, String method, long start) {
    log(log, method, start, false);
  }

  protected void log(Logger log, String method, long start, boolean debug) {
    String detail = logPrefix() + " [" + method + "] finished in [" + (System.currentTimeMillis() - start) + "] ms";
    if (debug) {
      if (log.isDebugEnabled()) {
        log.debug(detail);
      }
    } else if (log.isInfoEnabled()) {
      log.info(detail);
    }
  }

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
    throw new RuntimeException("Could not find avialable port");
  }

  public static final String SERVER_BIND_IP = "127.0.0.1";
  public static final AtomicInteger SERVER_BIND_PORT_START = new AtomicInteger(25000);
  public static final int SERVER_BIND_PORT_FINISH = 25100;

}
