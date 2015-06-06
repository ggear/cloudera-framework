package com.cloudera.framework.main.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

/**
 * Mini-cluster DFS, MR and Hive unit tests
 */
public class MiniClusterDfsMrHiveTest extends MiniClusterDfsMrHiveBaseTest {

  /**
   * Test Hive
   *
   * @throws Exception
   */
  @Test
  public void testHiveCreateSelect() throws Exception {
    new File(BaseTest.PATH_FS_LOCAL).mkdirs();
    File localDataFile = new File(BaseTest.PATH_FS_LOCAL + "/somedata.csv");
    BufferedWriter writer = new BufferedWriter(new FileWriter(localDataFile));
    writer.write("1,1\n");
    writer.write("2,2\n");
    writer.write("3,3\n");
    writer.close();
    processStatement("/com/cloudera/framework/main/test/ddl", "create.sql");
    processStatement("LOAD DATA LOCAL INPATH '" + localDataFile.toString()
        + "' OVERWRITE INTO TABLE somedata");
    Assert.assertEquals("3",
        processStatement("SELECT count(1) AS cnt FROM somedata").get(0));
    Assert.assertEquals("2",
        processStatement("SELECT col1 FROM somedata WHERE col2 = 2").get(0));
    Assert.assertEquals(1, processStatement("SHOW TABLES").size());
  }

  /**
   * Test Hive
   *
   * @throws Exception
   */
  @Test
  public void testHiveCreateSelectAgain() throws Exception {
    new File(BaseTest.PATH_FS_LOCAL).mkdirs();
    File localDataFile = new File(BaseTest.PATH_FS_LOCAL + "/somedata.csv");
    BufferedWriter writer = new BufferedWriter(new FileWriter(localDataFile));
    writer.write("1,1\n");
    writer.write("2,2\n");
    writer.write("3,3\n");
    writer.close();
    processStatement("/com/cloudera/framework/main/test/ddl", "create.sql");
    processStatement("LOAD DATA LOCAL INPATH '" + localDataFile.toString()
        + "' OVERWRITE INTO TABLE somedata");
    Assert.assertEquals("3",
        processStatement("SELECT count(1) AS cnt FROM somedata").get(0));
    Assert.assertEquals("2",
        processStatement("SELECT col1 FROM somedata WHERE col2 = 2").get(0));
    Assert.assertEquals(1, processStatement("SHOW TABLES").size());
  }

  /**
   * Test Hive is a clean
   *
   * @throws Exception
   */
  @Test
  public void testHiveClean() throws Exception {
    Assert.assertEquals(0, processStatement("SHOW TABLES").size());
  }

  /**
   * Test DFS mkdir and file touch
   *
   * @throws IOException
   */
  @Test
  public void testDfsMkDir() throws Exception {
    Assert.assertFalse(getFileSystem().exists(
        new Path(getPathDfs("/some_dir/some_file"))));
    Assert
        .assertTrue(getFileSystem().mkdirs(new Path(getPathDfs("/some_dir"))));
    Assert.assertTrue(getFileSystem().createNewFile(
        new Path(getPathDfs("/some_dir/some_file"))));
    Assert.assertTrue(getFileSystem().exists(
        new Path(getPathDfs("/some_dir/some_file"))));
  }

  /**
   * Test DFS is clean
   *
   * @throws IOException
   */
  @Test
  public void testDfsClean() throws Exception {
    Assert.assertFalse(getFileSystem().exists(
        new Path(getPathDfs("/some_dir/some_file"))));
  }

  /**
   * Test path generation relative to DFS root
   *
   * @throws Exception
   */
  @Test
  public void testPathDfs() throws Exception {
    Assert.assertEquals("", getPathDfs(""));
    Assert.assertEquals("/", getPathDfs("/"));
    Assert.assertEquals("//", getPathDfs("//"));
    Assert.assertEquals("tmp", getPathDfs("tmp"));
    Assert.assertEquals("/tmp", getPathDfs("/tmp"));
    Assert.assertEquals("//tmp", getPathDfs("//tmp"));
    Assert.assertEquals("///tmp", getPathDfs("///tmp"));
    Assert.assertEquals("///tmp//tmp", getPathDfs("///tmp//tmp"));
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
