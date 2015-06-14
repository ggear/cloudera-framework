package com.cloudera.framework.main.common;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.framework.main.test.LocalClusterDfsMrBaseTest;

public class DriverTest extends LocalClusterDfsMrBaseTest {

  private Driver driver;

  @Before
  public void setUpDriver() throws Exception {
    driver = new DriverNoOp(getConf());
  }

  @Test
  public void testRunner() throws Exception {
    Assert.assertEquals(Driver.RETURN_SUCCESS, driver.runner(new String[0]));
  }

  private class DriverNoOp extends Driver {

    public DriverNoOp(Configuration confguration) {
      super(confguration);
    }

    @Override
    public int execute() {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
      }
      return RETURN_SUCCESS;
    }

  }
}
