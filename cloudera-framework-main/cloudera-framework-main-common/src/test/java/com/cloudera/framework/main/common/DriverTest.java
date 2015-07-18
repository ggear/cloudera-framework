package com.cloudera.framework.main.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Assert;
import org.junit.Test;

import com.cloudera.framework.main.test.MiniClusterDfsMrTest;

public class DriverTest extends MiniClusterDfsMrTest {

  @Test
  public void testRunnerSuccessParameters() throws Exception {
    Driver driver = new CountFilesDriver(getConf());
    Assert.assertEquals(Driver.RETURN_SUCCESS, driver.runner(new String[] { "false" }));
  }

  @Test
  public void testRunnerSuccessOptions() throws Exception {
    Driver driver = new CountFilesDriver(getConf());
    driver.getConf().setBoolean("i.should.fail.option", false);
    Assert.assertEquals(Driver.RETURN_SUCCESS, driver.runner(new String[] { "false" }));
  }

  @Test
  public void testRunnerFailureParameters() throws Exception {
    Driver driver = new CountFilesDriver(getConf());
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME, driver.runner(new String[] { "true" }));
  }

  @Test
  public void testRunnerFailureOptions() throws Exception {
    Driver driver = new CountFilesDriver(getConf());
    driver.getConf().setBoolean("i.should.fail.option", true);
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME, driver.runner(new String[] { "false" }));
  }

  private enum Counter {
    FILES_NUMBER
  };

  private class CountFilesDriver extends Driver {

    private boolean iShouldFailOption;
    private String iShouldFailParameter;

    public CountFilesDriver(Configuration confguration) {
      super(confguration);
    }

    @Override
    public String description() {
      return "A dummy driver that counts the numer of files stored in DFS";
    }

    @Override
    public String[] options() {
      return new String[] { "i.should.fail.option=true|false" };
    }

    @Override
    public String[] parameters() {
      return new String[] { "i.should.fail.parameter" };
    }

    @Override
    public int prepare(String... arguments) throws Exception {
      if (arguments == null || arguments.length != 1) {
        throw new Exception("Invalid number of arguments");
      }
      iShouldFailParameter = arguments[0];
      iShouldFailOption = getConf().getBoolean("i.should.fail.option", false);
      return RETURN_SUCCESS;
    }

    @Override
    public int execute() throws IOException {
      FileSystem fileSystem = FileSystem.newInstance(getConf());
      RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/"), true);
      while (files.hasNext()) {
        files.next();
        incrementCounter(Counter.FILES_NUMBER, 1);
      }
      return iShouldFailOption || iShouldFailParameter.toLowerCase().equals(Boolean.TRUE.toString().toLowerCase())
          ? RETURN_FAILURE_RUNTIME : RETURN_SUCCESS;
    }
  }
}
