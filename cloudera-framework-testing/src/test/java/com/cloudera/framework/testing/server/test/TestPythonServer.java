package com.cloudera.framework.testing.server.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.PythonServer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(TestRunner.class)
public class TestPythonServer implements TestConstants {

  @ClassRule
  public static final PythonServer pythonServer = PythonServer.getInstance();

  @Test(expected = IOException.class)
  public void testPythonNullFile() throws Exception {
    pythonServer.execute(null);
  }

  @Test(expected = IOException.class)
  public void testPythonNoFile() throws Exception {
    pythonServer.execute(new File("/../../src/script/python/some-non-existent-script.py"));
  }

  @Test
  public void testPythonRunFail() throws Exception {
    assertNotEquals(0, pythonServer.execute(null, new File(ABS_DIR_CLASSES_TEST, "/../../src/script/python/python.py"), null, null,
      null, true));
    assertNotEquals(0, pythonServer.execute(null, new File(ABS_DIR_CLASSES_TEST, "/../../src/script/python/python.py"),
      Arrays.asList("--non-existent-param", "MY PARAMETER!"), null, null, true));
  }

  @Test
  public void testPython() throws Exception {
    assertEquals(0, pythonServer.execute(null, new File(ABS_DIR_CLASSES_TEST, "/../../src/script/python/python.py"),
      Arrays.asList("--param", "MY PARAMETER!")));
  }

  @Test
  public void testPythonAgain() throws Exception {
    testPython();
  }

}
