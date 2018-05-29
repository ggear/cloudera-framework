package com.cloudera.framework.example.five.test;

import static org.junit.Assert.assertEquals;

import java.io.File;

import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.PythonServer;
import com.googlecode.zohhak.api.Coercion;
import com.googlecode.zohhak.api.TestWith;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.runner.RunWith;

/**
 * Test process
 */
@RunWith(TestRunner.class)
public class Process implements TestConstants {

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static final PythonServer pythonServer = PythonServer.getInstance();

  private static final String DATASET_DIR = "/tmp/stateunion";
  private static final String DATASET_LANDING_DIR = DATASET_DIR + "/landing";
  private static final String DATASET_PROCESSED_DIR = DATASET_DIR + "/processed/words";

  public final TestMetaData testMetaDataAll = TestMetaData.getInstance()
    .dataSetSourceDirs(REL_DIR_DATASET)
    .dataSetDestinationDirs(DATASET_LANDING_DIR);

  /**
   * Test process
   */
  // TODO: Re-enable once running on CentOS works
  @Ignore
  @TestWith({"testMetaDataAll"})
  public void testProcess(TestMetaData testMetaData) throws Exception {
    assertEquals(0, pythonServer.execute(ABS_DIR_PYTHON_BIN, new File(ABS_DIR_PYTHON_SRC, "process.py")));
    int messageCount = 0;
    for (Path path : dfsServer.listFilesDfs(DATASET_PROCESSED_DIR, true)) {
      messageCount += IOUtils.toString(dfsServer.getFileSystem().open(path)).split("\n").length;
    }
    assertEquals(4988, messageCount);
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
