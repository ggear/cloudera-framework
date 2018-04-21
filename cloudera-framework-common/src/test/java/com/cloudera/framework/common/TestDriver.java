package com.cloudera.framework.common;

import static com.cloudera.framework.common.Driver.CONF_CLDR_JOB_GROUP;
import static com.cloudera.framework.common.Driver.CONF_CLDR_JOB_NAME;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Map;

import com.cloudera.framework.common.Driver.Engine;
import com.cloudera.framework.common.navigator.MetaDataExecution;
import com.cloudera.framework.common.navigator.MetaDataTemplate;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MProperty;
import com.cloudera.nav.sdk.model.custom.CustomPropertyType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(TestRunner.class)
public class TestDriver {

  private static final Logger LOG = LoggerFactory.getLogger(TestDriver.class);

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  @Before
  public void cleanConf() {
    dfsServer.getConf().unset("i.should.fail.option");
  }

  @Test
  public void testRunnerFailureHadoop() throws Exception {
    Driver driver = new CountFilesDriver(dfsServer.getConf(), Engine.HADOOP);
    assertEquals(Driver.FAILURE_ARGUMENTS, driver.runner());
  }

  @Test
  public void testRunnerFailureSpark() throws Exception {
    Driver driver = new CountFilesDriver(dfsServer.getConf(), Engine.SPARK);
    assertEquals(Driver.FAILURE_ARGUMENTS, driver.runner());
  }

  @Test
  public void testRunnerSuccessParametersHadoop() throws Exception {
    Driver driver = new CountFilesDriver(dfsServer.getConf(), Engine.HADOOP);
    assertEquals(Driver.SUCCESS, driver.runner("false"));
  }

  @Test
  public void testRunnerSuccessOptionsHadoop() throws Exception {
    Driver driver = new CountFilesDriver(dfsServer.getConf(), Engine.HADOOP);
    driver.getConf().setBoolean("i.should.fail.option", false);
    assertEquals(Driver.SUCCESS, driver.runner("false"));
  }

  @Test
  public void testRunnerFailureParametersHadoop() throws Exception {
    Driver driver = new CountFilesDriver(dfsServer.getConf(), Engine.HADOOP);
    assertEquals(Driver.FAILURE_RUNTIME, driver.runner("true"));
  }

  @Test
  public void testRunnerFailureParametersSpark() throws Exception {
    Driver driver = new CountFilesDriver(dfsServer.getConf(), Engine.SPARK);
    assertEquals(Driver.FAILURE_RUNTIME, driver.runner("true"));
  }

  @Test
  public void testRunnerFailureOptionsHadoop() throws Exception {
    Driver driver = new CountFilesDriver(dfsServer.getConf(), Engine.HADOOP);
    driver.getConf().setBoolean("i.should.fail.option", true);
    assertEquals(Driver.FAILURE_RUNTIME, driver.runner("false"));
  }

  @Test
  public void testRunnerFailureOptionsSpark() throws Exception {
    Driver driver = new CountFilesDriver(dfsServer.getConf(), Engine.SPARK);
    assertEquals(Driver.FAILURE_RUNTIME, driver.runner("-Di.should.fail.option=true", "false"));
  }

  @Test
  public void testRunnerMetaData() throws Exception {
    Driver driver = new CountFilesDriver(dfsServer.getConf(), Engine.SPARK);
    assertEquals(Driver.SUCCESS, driver.runner("--" + CONF_CLDR_JOB_GROUP + "=some-test",
      "--" + CONF_CLDR_JOB_NAME + "=some-test", "false"));
  }

  @MClass(model = "some_test_template")
  public class SomeTestTemplate extends MetaDataTemplate {
    public SomeTestTemplate(Configuration conf) {
      super(conf);
    }
  }

  @MClass(model = "some_test_execution")
  public class SomeTestExecution extends MetaDataExecution {

    @MProperty(register = true, fieldType = CustomPropertyType.INTEGER)
    private int test;

    public SomeTestExecution(Configuration conf, SomeTestTemplate template, Instant started, Instant ended, int test) {
      super(conf, template, "1.0.0-SNAPSHOT");
      setStarted(started);
      setEnded(ended);
      this.test = test;
    }

    public int getTest() {
      return test;
    }

    public void setTest(int test) {
      this.test = test;
    }

  }

  private class CountFilesDriver extends Driver {

    private boolean iShouldFailOption;
    private String iShouldFailParameter;

    public CountFilesDriver(Configuration configuration, Engine engine) {
      super(configuration, engine);
    }

    public CountFilesDriver(Configuration configuration, Engine engine, boolean enableMetaData) {
      super(configuration, engine, enableMetaData);
    }

    @Override
    public String description() {
      return "A dummy driver that counts the number of files stored in DFS";
    }

    @Override
    public String[] options() {
      return new String[]{"i.should.fail.option=true|false"};
    }

    @Override
    public String[] parameters() {
      return new String[]{"i.should.fail.parameter"};
    }

    @Override
    public int prepare(String... arguments) throws Exception {
      if (arguments == null || arguments.length != 1) {
        return FAILURE_ARGUMENTS;
      }
      iShouldFailParameter = arguments[0];
      iShouldFailOption = getConf().getBoolean("i.should.fail.option", false);
      return SUCCESS;
    }

    @Override
    public int execute() throws IOException {
      FileSystem fileSystem = FileSystem.newInstance(getConf());
      RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("."), true);
      while (files.hasNext()) {
        files.next();
        incrementCounter(Counter.FILES_IN, 1);
      }
      String tag = "A_TEST_TAG";
      SomeTestExecution metadata = new SomeTestExecution(getConf(), new SomeTestTemplate(getConf()),
        Instant.now(), Instant.now(), 200);
      metadata.addTags(tag);
      addMetaData(metadata);
      for (Map<String, Object> entity : getMetaData(metadata, tag)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Found entity " + entity);
        }
      }
      return iShouldFailOption || iShouldFailParameter.toLowerCase().equals(Boolean.TRUE.toString().toLowerCase()) ?
        FAILURE_RUNTIME : SUCCESS;
    }

  }

}
