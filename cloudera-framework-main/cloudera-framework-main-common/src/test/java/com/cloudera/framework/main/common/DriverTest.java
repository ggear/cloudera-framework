package com.cloudera.framework.main.common;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import com.cloudera.framework.main.test.MiniClusterDfsMrBaseTest;

public class DriverTest extends MiniClusterDfsMrBaseTest {

  @Test
  public void testRunnerSuccessParameters() throws Exception {
    Driver driver = new DriverNoOp(getConf());
    Assert.assertEquals(Driver.RETURN_SUCCESS,
        driver.runner(new String[] { "false" }));
  }

  @Test
  public void testRunnerSuccessOptions() throws Exception {
    Driver driver = new DriverNoOp(getConf());
    driver.getConf().setBoolean("i.should.fail.option", false);
    Assert.assertEquals(Driver.RETURN_SUCCESS,
        driver.runner(new String[] { "false" }));
  }

  @Test
  public void testRunnerFailureParameters() throws Exception {
    Driver driver = new DriverNoOp(getConf());
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME,
        driver.runner(new String[] { "true" }));
  }

  @Test
  public void testRunnerFailureOptions() throws Exception {
    Driver driver = new DriverNoOp(getConf());
    driver.getConf().setBoolean("i.should.fail.option", true);
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME,
        driver.runner(new String[] { "false" }));
  }

  private enum Counter {
    TEST
  };

  private class DriverNoOp extends Driver {

    private boolean iShouldFailOption;
    private String iShouldFailParameter;

    public DriverNoOp(Configuration confguration) {
      super(confguration);
    }

    @Override
    public String description() {
      return "A dummy driver";
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
    public int execute() {
      incrementCounter(Counter.TEST, 1);
      return iShouldFailOption
          || iShouldFailParameter.toLowerCase().equals(
              Boolean.TRUE.toString().toLowerCase()) ? RETURN_FAILURE_RUNTIME
          : RETURN_SUCCESS;
    }
  }
}
