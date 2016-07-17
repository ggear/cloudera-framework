package com.cloudera.framework.testing.server.tests;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.PythonServer;

@RunWith(TestRunner.class)
public class TestPythonServer implements TestConstants {

  @ClassRule
  public static PythonServer pythonServer = PythonServer.getInstance();

  @Test(expected = IOException.class)
  public void testPythonNullFile() throws Exception {
    pythonServer.execute(null);
  }

  @Test(expected = IOException.class)
  public void testPythonNoFile() throws Exception {
    pythonServer.execute(new File("/python/some-non-existant-script.py"));
  }

  @Test
  public void testPythonRunFail() throws Exception {
    assertFalse(pythonServer.execute(new File(ABS_DIR_CLASSES_TEST, "/python/python.py"), null, null, null, null, true) == 0);
    assertFalse(pythonServer.execute(new File(ABS_DIR_CLASSES_TEST, "/python/python.py"),
        Arrays.asList(new String[] { "--non-existant-param", "MY PARAMETER!" }), null, null, null, true) == 0);
  }

  @Test
  public void testPython() throws Exception {
    assertTrue(pythonServer.execute(new File(ABS_DIR_CLASSES_TEST, "/python/python.py"),
        Arrays.asList(new String[] { "--param", "MY PARAMETER!" })) == 0);
  }

  @Test
  public void testPythonAgain() throws Exception {
    testPython();
  }

}
