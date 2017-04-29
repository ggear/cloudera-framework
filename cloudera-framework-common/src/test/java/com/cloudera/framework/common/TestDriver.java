package com.cloudera.framework.common;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(TestRunner.class)
public class TestDriver {

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  @Test
  public void testRunnerSuccessParameters() throws Exception {
    Driver driver = new CountFilesDriver(dfsServer.getConf());
    assertEquals(Driver.RETURN_SUCCESS, driver.runner(new String[]{"false"}));
  }

  @Test
  public void testRunnerSuccessOptions() throws Exception {
    Driver driver = new CountFilesDriver(dfsServer.getConf());
    driver.getConf().setBoolean("i.should.fail.option", false);
    assertEquals(Driver.RETURN_SUCCESS, driver.runner(new String[]{"false"}));
  }

  @Test
  public void testRunnerFailureParameters() throws Exception {
    Driver driver = new CountFilesDriver(dfsServer.getConf());
    assertEquals(Driver.RETURN_FAILURE_RUNTIME, driver.runner(new String[]{"true"}));
  }

  @Test
  public void testRunnerFailureOptions() throws Exception {
    Driver driver = new CountFilesDriver(dfsServer.getConf());
    driver.getConf().setBoolean("i.should.fail.option", true);
    assertEquals(Driver.RETURN_FAILURE_RUNTIME, driver.runner(new String[]{"false"}));
  }

  private enum Counter {
    FILES_NUMBER
  }

  private class CountFilesDriver extends Driver {

    private boolean iShouldFailOption;
    private String iShouldFailParameter;

    public CountFilesDriver(Configuration configuration) {
      super(configuration);
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
        throw new Exception("Invalid number of arguments");
      }
      iShouldFailParameter = arguments[0];
      iShouldFailOption = getConf().getBoolean("i.should.fail.option", false);
      return RETURN_SUCCESS;
    }

    @Override
    public int execute() throws IOException {
      FileSystem fileSystem = FileSystem.newInstance(getConf());
      RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("."), true);
      while (files.hasNext()) {
        files.next();
        incrementCounter(Counter.FILES_NUMBER, 1);
      }
      return iShouldFailOption || iShouldFailParameter.toLowerCase().equals(Boolean.TRUE.toString().toLowerCase()) ? RETURN_FAILURE_RUNTIME
        : RETURN_SUCCESS;
    }

  }

}
