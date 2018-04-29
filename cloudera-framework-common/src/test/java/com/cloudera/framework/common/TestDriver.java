package com.cloudera.framework.common;

import static com.cloudera.framework.common.Driver.CONF_CLDR_JOB_GROUP;
import static com.cloudera.framework.common.Driver.CONF_CLDR_JOB_METADATA;
import static com.cloudera.framework.common.Driver.CONF_CLDR_JOB_NAME;
import static com.cloudera.framework.common.Driver.CONF_CLDR_JOB_TRANSACTION;
import static com.cloudera.framework.common.Driver.METADATA_NAMESPACE;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import com.cloudera.framework.common.Driver.Engine;
import com.cloudera.framework.common.navigator.MetaDataExecution;
import com.cloudera.framework.common.navigator.MetaDataTemplate;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MProperty;
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
    assertEquals(Driver.FAILURE_RUNTIME, driver.runner("--i.should.fail.option=true", "false"));
  }

  @Test
  public void testRunnerMetaData() throws Exception {
    Driver driver = new CountFilesDriver(dfsServer.getConf(), Engine.SPARK);
    driver.getConf().set(CONF_CLDR_JOB_GROUP, "test-" + METADATA_NAMESPACE.replace("_", "-"));
    driver.getConf().set(CONF_CLDR_JOB_NAME, "test-" + METADATA_NAMESPACE.replace("_", "-") + "-job");
    driver.getConf().set(CONF_CLDR_JOB_TRANSACTION, UUID.randomUUID().toString());
    driver.getConf().set(CONF_CLDR_JOB_METADATA, "true");
    MetaDataExecution metaData = new TestExecution(driver.getConf(), new TestTemplate(driver.getConf()), 0, null, null);
    driver.pollMetaData(metaData);
    assertEquals(0, driver.pullMetaData(metaData).size());
    assertEquals(Driver.SUCCESS, driver.runner("false"));
    assertEquals(driver.getConf().getBoolean(CONF_CLDR_JOB_METADATA, false) ? 1 : 0, driver.pullMetaData(metaData).size());
  }

  @MClass(model = "test_template")
  public class TestTemplate extends MetaDataTemplate {
    public TestTemplate(Configuration conf) {
      super(conf);
    }
  }

  @MClass(model = "test_execution")
  public class TestExecution extends MetaDataExecution {

    @MProperty(attribute = "FILES_IN")
    private String filesIn;

    private TestExecution() {
    }

    public TestExecution(Configuration conf, TestTemplate template, Integer exit, Instant started, Instant ended) {
      super(conf, template, exit);
      setStarted(started);
      setEnded(ended);
    }

    @Override
    public MetaDataExecution clone(MetaDataExecution metaData, Map<String, Object> metaDataMap, String string) {
      TestExecution clone = new TestExecution();
      clone.update(metaData, metaDataMap, string);
      clone.setFilesIn(((Map) metaDataMap.get("properties")).get("FILES_IN").toString());
      return clone;
    }

    public String getFilesIn() {
      return filesIn;
    }

    public void setFilesIn(String filesIn) {
      this.filesIn = filesIn;
    }

  }

  private class CountFilesDriver extends Driver {

    private boolean iShouldFailOption;
    private String iShouldFailParameter;

    public CountFilesDriver(Configuration configuration, Engine engine) {
      super(configuration, engine);
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
      int filesIn = 0;
      RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("."), true);
      while (files.hasNext()) {
        files.next();
        filesIn++;
      }
      MetaDataExecution metaData = new TestExecution(getConf(), new TestTemplate(getConf()), 100, Instant.now(), Instant.now());
      metaData.addTags("STAGING", "UNVETTED");
      addMetaDataCounter(metaData, Counter.FILES_IN, filesIn);
      pushMetaData(metaData);
      pushMetadataTags(metaData, "UNVETTED", "VETTED");
      if (LOG.isDebugEnabled()) {
        LOG.debug("Driver metadata " + pullMetaData(metaData));
      }
      return iShouldFailOption || iShouldFailParameter.toLowerCase().equals(Boolean.TRUE.toString().toLowerCase()) ?
        FAILURE_RUNTIME : SUCCESS;
    }

  }

}
