package com.cloudera.framework.common;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import com.cloudera.framework.common.Driver.Engine;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(TestRunner.class)
public class TestDriver {

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
    driver.getConf().setBoolean("i.should.fail.option", true);
    assertEquals(Driver.FAILURE_RUNTIME, driver.runner("false"));
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
      RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("."), true);
      while (files.hasNext()) {
        files.next();
        incrementCounter(Counter.FILES_IN, 1);
      }
      return iShouldFailOption || iShouldFailParameter.toLowerCase().equals(Boolean.TRUE.toString().toLowerCase()) ?
        FAILURE_RUNTIME : SUCCESS;
    }

  }

}
