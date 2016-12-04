package com.cloudera.test;

import java.io.File;

import com.cloudera.TestBase;
import com.cloudera.TestConstants;
import com.cloudera.framework.common.util.FsUtil;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.PythonServer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Python script test
 */
@RunWith(TestRunner.class)
public class Python extends TestBase {

  @ClassRule
  public static PythonServer pythonServer = PythonServer.getInstance();

  /**
   * Test all scripts in the {@link TestConstants#ABS_DIR_PYTHON} directory, in
   * serial, lexicographic file name order
   *
   * @throws Exception if any script fails to run
   */
  @Test
  public void test() throws Exception {
    for (File script : FsUtil.listFiles(ABS_DIR_PYTHON)) {
      pythonServer.execute(script);
    }
  }

}
