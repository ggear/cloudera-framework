package com.cloudera.framework.main.test;

import java.io.IOException;
import java.lang.reflect.Method;

import junit.extensions.TestSetup;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.mapred.HadoopTestCase;
import org.junit.After;
import org.junit.Before;

public abstract class LocalClusterDFSMRTest extends HadoopTestCase {

  public static final int DEFAULT_TASKTRACKERS = 1;
  public static final int DEFAULT_DATANODES = 1;

  public LocalClusterDFSMRTest() throws IOException {
    super(HadoopTestCase.LOCAL_MR, HadoopTestCase.LOCAL_FS,
        DEFAULT_TASKTRACKERS, DEFAULT_DATANODES);
  }

  public LocalClusterDFSMRTest(int mrMode, int taskTrackers, int dataNodes)
      throws IOException {
    super(mrMode, HadoopTestCase.LOCAL_FS, taskTrackers, dataNodes);
  }

  public void setUpClass() throws Exception {
  }

  public void tearDownClass() throws Exception {
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // BaseTest.setUp(getFileSystem());
  }

  @After
  @Override
  public void tearDown() throws Exception {
    // BaseTest.tearDown(getFileSystem());
    super.tearDown();
  }

  public junit.framework.Test getTestSuiteWithClassLifecycleMethods()
      throws Exception {
    TestSuite suite = new TestSuite();
    for (Method method : getClass().getMethods()) {
      if (method.getName().startsWith("test")) {
        TestCase testCase = getClass().getConstructor().newInstance();
        testCase.setName(method.getName());
        suite.addTest(testCase);
      }
    }
    TestSetup wrapper = new TestSetup(suite) {
      @Override
      protected void setUp() throws Exception {
        setUpClass();
      }

      @Override
      protected void tearDown() throws Exception {
        tearDownClass();
      }
    };
    return wrapper;
  }

}
