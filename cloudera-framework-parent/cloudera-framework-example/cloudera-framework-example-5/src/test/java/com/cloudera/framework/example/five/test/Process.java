package com.cloudera.framework.example.five.test;

import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.PythonServer;
import com.googlecode.zohhak.api.Coercion;
import org.junit.ClassRule;
import org.junit.Test;
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

  /**
   * Test process
   */
  @Test
  public void testProcess() throws Exception {
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
