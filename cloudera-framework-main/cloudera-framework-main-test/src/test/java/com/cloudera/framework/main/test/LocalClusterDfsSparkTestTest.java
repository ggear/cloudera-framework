package com.cloudera.framework.main.test;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

/**
 * LocalClusterDfsSparkTest system test
 */
public class LocalClusterDfsSparkTestTest extends LocalClusterDfsSparkTest {

  /**
   * Test Spark
   *
   * @throws Exception
   */
  @Test
  public void testSpark() throws Exception {
    // TODO: Provide impl
  }

  /**
   * Test Spark
   *
   * @throws Exception
   */
  @Test
  public void testSparkAgain() throws Exception {
    // TODO: Provide impl
  }

  /**
   * Test DFS mkdir and file touch
   *
   * @throws IOException
   */
  @Test
  public void testDfsMkDir() throws Exception {
    Assert.assertFalse(new File(getPathLocal("/some_dir/some_file")).exists());
    Assert.assertTrue(getFileSystem().mkdirs(new Path(getPathDfs("/some_dir"))));
    Assert.assertTrue(getFileSystem().createNewFile(new Path(getPathDfs("/some_dir/some_file"))));
    Assert.assertFalse(new File(getPathLocal("/some_dir/some_file")).exists());
  }

  /**
   * Test DFS is clean
   *
   * @throws IOException
   */
  @Test
  public void testDfsClean() throws IOException {
    Assert.assertFalse(new File(getPathLocal("/some_dir/some_file")).exists());
  }

  /**
   * Test path generation relative to DFS root
   *
   * @throws Exception
   */
  @Test
  public void testPathDfs() throws Exception {
    Assert.assertEquals(BaseTest.REL_DIR_DFS_LOCAL, getPathDfs(""));
    Assert.assertEquals(BaseTest.REL_DIR_DFS_LOCAL, getPathDfs("/"));
    Assert.assertEquals(BaseTest.REL_DIR_DFS_LOCAL, getPathDfs("//"));
    Assert.assertEquals(BaseTest.REL_DIR_DFS_LOCAL + "/tmp", getPathDfs("tmp"));
    Assert.assertEquals(BaseTest.REL_DIR_DFS_LOCAL + "/tmp", getPathDfs("/tmp"));
    Assert.assertEquals(BaseTest.REL_DIR_DFS_LOCAL + "/tmp", getPathDfs("//tmp"));
    Assert.assertEquals(BaseTest.REL_DIR_DFS_LOCAL + "/tmp", getPathDfs("///tmp"));
    Assert.assertEquals(BaseTest.REL_DIR_DFS_LOCAL + "/tmp/tmp", getPathDfs("///tmp//tmp"));
  }

  /**
   * Test path generation relative to module root
   *
   * @throws Exception
   */
  @Test
  public void testPathLocal() throws Exception {
    String localDir = new File(".").getAbsolutePath();
    localDir = localDir.substring(0, localDir.length() - 2);
    Assert.assertEquals(localDir, getPathLocal(""));
    Assert.assertEquals(localDir, getPathLocal("/"));
    Assert.assertEquals(localDir, getPathLocal("//"));
    Assert.assertEquals(localDir + "/tmp", getPathLocal("tmp"));
    Assert.assertEquals(localDir + "/tmp", getPathLocal("/tmp"));
    Assert.assertEquals(localDir + "/tmp", getPathLocal("//tmp"));
    Assert.assertEquals(localDir + "/tmp", getPathLocal("///tmp"));
    Assert.assertEquals(localDir + "/tmp/tmp", getPathLocal("///tmp//tmp"));
  }

}
