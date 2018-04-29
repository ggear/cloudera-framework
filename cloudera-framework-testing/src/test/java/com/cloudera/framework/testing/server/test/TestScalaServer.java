package com.cloudera.framework.testing.server.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.ScalaServer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(TestRunner.class)
public class TestScalaServer implements TestConstants {

  @ClassRule
  public static final ScalaServer scalaServer = ScalaServer.getInstance();

  @Test(expected = IOException.class)
  public void testScalaNullFile() throws Exception {
    scalaServer.execute(null);
  }

  @Test(expected = IOException.class)
  public void testScalaNoFile() throws Exception {
    scalaServer.execute(new File("/../../src/script/scala/some-non-existent-script.py"));
  }

  @Test
  public void testScalaRunFail() throws Exception {
    assertNotEquals(0, scalaServer.execute(null, new File(ABS_DIR_CLASSES_TEST, "/../../src/script/scala/scala.scala"), null,
      Maps.newHashMap(ImmutableMap.of("KILL_MY_SCRIPT", "true")), null, true));
  }

  @Test
  public void testScala() throws Exception {
    assertEquals(0, scalaServer.execute(null, new File(ABS_DIR_CLASSES_TEST, "/../../src/script/scala/scala.scala")));
  }

  @Test
  public void testScalaAgain() throws Exception {
    testScala();
  }

}
