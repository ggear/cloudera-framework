package com.cloudera.framework.testing;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import com.cloudera.framework.testing.server.CdhServer;
import com.cloudera.framework.testing.server.DfsServer;
import com.googlecode.zohhak.api.runners.ZohhakRunner;
import com.googlecode.zohhak.internal.junit.ParametrizedFrameworkMethod;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.internal.runners.model.ReflectiveCallable;
import org.junit.internal.runners.statements.Fail;
import org.junit.rules.ExternalResource;
import org.junit.rules.RunRules;
import org.junit.rules.TestRule;
import org.junit.runner.Runner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;
import parquet.Log;
import parquet.hadoop.ParquetOutputFormat;
import uk.org.lidalia.sysoutslf4j.context.SysOutOverSLF4J;

/**
 * JUnit {@link Runner} required for all tests as per:
 * <p>
 * <pre>
 * &#64;RunWith(TestRunner.class)
 * public class MyTest {
 * }
 * </pre>
 */
public class TestRunner extends ZohhakRunner implements TestConstants {

  private static final Logger LOG = LoggerFactory.getLogger(TestRunner.class);

  static {
    Log.getLog(ParquetOutputFormat.class);
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
    SysOutOverSLF4J.sendSystemOutAndErrToSLF4J();
    System.setProperty("java.security.krb5.realm", "CDHCLUSTER.com");
    System.setProperty("java.security.krb5.kdc", "kdc.cdhcluster.com");
    System.setProperty("java.security.krb5.conf", "/dev/null");
    System.setProperty("dir.working", ABS_DIR_WORKING);
    System.setProperty("dir.working.target", ABS_DIR_TARGET);
    System.setProperty("dir.working.target.hdfs", ABS_DIR_DFS_LOCAL);
    System.setProperty("test.build.data", ABS_DIR_DFS);
    System.setProperty("hadoop.tmp.dir", ABS_DIR_DFS_TMP);
    System.setProperty("dir.working.target.derby", ABS_DIR_DERBY);
    System.setProperty("dir.working.target.derby.db", ABS_DIR_DERBY_DB);
    System.setProperty("derby.stream.error.file", ABS_DIR_DERBY_LOG);
  }

  public TestRunner(Class<?> clazz) throws InitializationError {
    super(clazz);
  }

  /**
   * Dereference an instance <code>field</code> of <code>type</code> from an
   * <code>object</code>
   */
  public static synchronized Object toObject(Object object, String field, Class<?> type) {
    Object dereferencedObject = null;
    try {
      if (field != null) {
        Field dereferencedField = object.getClass().getField(field);
        if (!Modifier.isFinal(dereferencedField.getModifiers())) {
          throw new IllegalArgumentException("All fields should be final");
        }
        dereferencedObject = dereferencedField.get(object);
        if (type != null) {
          if (!type.isInstance(dereferencedObject)) {
            throw new IllegalArgumentException("Unexpected field type");
          }
        }
      } else if (type != null) {
        for (Field fields : object.getClass().getFields()) {
          if (fields.getType().equals(type)) {
            dereferencedObject = fields.get(object);
            break;
          }
        }
      } else {
        throw new IllegalArgumentException("Either field or type is required to be non-null");
      }
      if (dereferencedObject == null) {
        throw new IllegalArgumentException("Could not find field");
      }
    } catch (Exception exception) {
      String message = "Could not find field [" + field + "] with type [" + type + "] on object [" + object + "] with class ["
        + (object == null ? "null" : object.getClass()) + "]";
      if (LOG.isErrorEnabled()) {
        LOG.error(message, exception);
      }
      throw new RuntimeException(message, exception);
    }
    return dereferencedObject;
  }

  /**
   * Dereference an instance {@link TestMetaData} <code>field</code> from an
   * <code>object</code>
   */
  public static TestMetaData toCdhMetaData(Object object, String field) {
    TestMetaData cdhMetaData = (TestMetaData) toObject(object, field, TestMetaData.class);
    if (cdhMetaData.getDataSetSourceDirs() != null && cdhMetaData.getDataSetDestinationDirs() != null
      && DfsServer.getInstance().isStarted()) {
      try {
        DfsServer.getInstance().copyFromLocalDir(cdhMetaData.getDataSetSourceDirs(), cdhMetaData.getDataSetDestinationDirs(),
          cdhMetaData.getDataSetNames(), cdhMetaData.getDataSetSubsets(), cdhMetaData.getDataSetLabels());
      } catch (Exception exception) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Could not initialise [" + object.getClass().getSimpleName() + "]", exception);
        }
      }
    }
    return cdhMetaData;
  }

  @Override
  @SuppressWarnings("deprecation")
  protected Statement methodBlock(FrameworkMethod method) {
    Object test;
    try {
      test = new ReflectiveCallable() {
        @Override
        protected Object runReflectiveCall() throws Throwable {
          return createTest();
        }
      }.run();
    } catch (Throwable e) {
      return new Fail(e);
    }
    boolean validated = true;
    for (CdhServer cdhServer : getTestClass().getAnnotatedFieldValues(test, ClassRule.class, CdhServer.class)) {
      validated = validated && cdhServer.isValid();
      if (validated) {
        for (CdhServer cdhServerDependency : cdhServer.getDependencies()) {
          validated = validated && cdhServerDependency.isValid();
        }
      }
    }
    Statement statement;
    if (validated) {
      statement = methodInvoker(method, test);
      statement = withLogging(method, test, statement);
      statement = possiblyExpectingExceptions(method, test, statement);
      statement = withPotentialTimeout(method, test, statement);
      statement = withBefores(method, test, statement);
      statement = withAfters(method, test, statement);
      statement = withServerRules(method, test, statement);
      statement = withRules(method, test, statement);
    } else {
      statement = new Statement() {
        @Override
        public void evaluate() throws Throwable {
          if (LOG.isWarnEnabled()) {
            LOG.warn("Skipping [" + method.getDeclaringClass().getCanonicalName() + "." + method.getName() + "], invalid classpath");
          }
        }
      };
    }
    return statement;
  }

  private String getDescription(final FrameworkMethod method) {
    return method.getDeclaringClass().getSimpleName() + "." + method.getName() +
      (method instanceof ParametrizedFrameworkMethod ? ("(" + ((ParametrizedFrameworkMethod) method).getParametersLine() + ")") : "");
  }

  private Statement withLogging(final FrameworkMethod method, Object target, Statement statement) {
    final AtomicLong time = new AtomicLong();
    List<TestRule> rules = new ArrayList<>();
    rules.add(new ExternalResource() {
      @Override
      protected void before() throws Throwable {
        if (LOG.isDebugEnabled()) {
          time.set(System.currentTimeMillis());
          LOG.debug("Beginning [" + getDescription(method) + "]");
        }
      }

      @Override
      protected void after() {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Completed [" + getDescription(method) + "] in ["
            + (System.currentTimeMillis() - time.get()) + "] ms");
        }
      }
    });
    return new RunRules(statement, rules, getDescription());
  }

  @SuppressWarnings({"rawtypes"})
  private Statement withServerRules(FrameworkMethod method, Object target, Statement statement) {
    final Set<CdhServer> servers = new TreeSet<>();
    servers.addAll(getTestClass().getAnnotatedFieldValues(target, Rule.class, CdhServer.class));
    servers.addAll(getTestClass().getAnnotatedFieldValues(target, ClassRule.class, CdhServer.class));
    List<CdhServer> dependencies = new ArrayList<>();
    for (CdhServer server : servers) {
      Collections.addAll(dependencies, server.getDependencies());
    }
    servers.addAll(dependencies);
    List<TestRule> rules = new ArrayList<>();
    rules.add(new ExternalResource() {
      @Override
      protected void before() throws Throwable {
        for (CdhServer server : servers) {
          if (server.isStarted()) {
            server.clean();
          }
        }
      }

      @Override
      protected void after() {
        for (CdhServer server : servers) {
          try {
            if (server.isStarted()) {
              server.state();
            }
          } catch (Exception exception) {
            throw new RuntimeException("Failure to run state", exception);
          }
        }
      }
    });
    return new RunRules(statement, rules, getDescription());
  }

  private Statement withRules(FrameworkMethod method, Object target, Statement statement) {
    List<TestRule> testRules = getTestRules(target);
    Statement result = statement;
    result = withMethodRules(method, testRules, target, result);
    result = withTestRules(method, testRules, result);
    return result;
  }

  @SuppressWarnings("SuspiciousMethodCalls")
  private Statement withMethodRules(FrameworkMethod method, List<TestRule> testRules, Object target, Statement result) {
    for (org.junit.rules.MethodRule each : rules(target)) {
      if (!testRules.contains(each)) {
        result = each.apply(result, method, target);
      }
    }
    return result;
  }

  private Statement withTestRules(FrameworkMethod method, List<TestRule> testRules, Statement statement) {
    return testRules.isEmpty() ? statement : new RunRules(statement, testRules, describeChild(method));
  }

}
