package com.cloudera.test;

import java.io.File;

import com.cloudera.TestBase;
import com.cloudera.TestConstants;
import com.cloudera.framework.common.util.FsUtil;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.HiveServer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Hive script test
 */
@RunWith(TestRunner.class)
public class Hive extends TestBase {

  @ClassRule
  public static HiveServer hiveServer = HiveServer.getInstance();

  /**
   * Test all scripts in the {@link TestConstants#ABS_DIR_HIVE_SCHEMA},
   * {@link TestConstants#ABS_DIR_HIVE_REFRESH} and
   * {@link TestConstants#ABS_DIR_HIVE_QUERY} directories, in serial,
   * lexicographic file name order
   *
   * @throws Exception if any script fails to compile and run
   */
  @Test
  public void test() throws Exception {
    for (File script : FsUtil.listFiles(ABS_DIR_HIVE_SCHEMA, ABS_DIR_HIVE_REFRESH, ABS_DIR_HIVE_QUERY)) {
      hiveServer.execute(script);
    }
  }

}
