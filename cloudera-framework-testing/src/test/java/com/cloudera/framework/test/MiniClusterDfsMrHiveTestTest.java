package com.cloudera.framework.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import com.cloudera.framework.testing.BaseTest;
import com.cloudera.framework.testing.MiniClusterDfsMrHiveTest;
import com.google.common.collect.ImmutableMap;

/**
 * MiniClusterDfsMrHiveTest system test
 */
public class MiniClusterDfsMrHiveTestTest extends MiniClusterDfsMrHiveTest {

  /**
   * Test Hive
   *
   * @throws Exception
   */
  @Test
  public void testHiveCreateSelect() throws Exception {
    new File(BaseTest.REL_DIR_FS_LOCAL).mkdirs();
    File localDataFile = new File(BaseTest.REL_DIR_FS_LOCAL + "/somedata.csv");
    BufferedWriter writer = new BufferedWriter(new FileWriter(localDataFile));
    writer.write("1,1\n");
    writer.write("2,2\n");
    writer.write("3,3\n");
    writer.close();
    Assert.assertEquals(2, processStatement("/ddl", "create.sql", new ImmutableMap.Builder<String, String>()
        .put("test.table.name", "somedata").put("test.table.field.delim", ",").build()).size());
    Assert.assertEquals(0,
        processStatement("LOAD DATA LOCAL INPATH '" + localDataFile.toString() + "' OVERWRITE INTO TABLE somedata")
            .size());
    Assert.assertEquals("3", processStatement("SELECT count(1) AS cnt FROM somedata").get(0));
    Assert.assertEquals("2", processStatement("SELECT col1 FROM somedata WHERE col2 = 2").get(0));
    Assert.assertEquals(2, processStatement("SELECT * FROM somedata", Collections.<String, String> emptyMap(),
        Collections.<String, String> emptyMap(), 2).size());
    Assert.assertEquals(1, processStatement("SHOW TABLES").size());
  }

  /**
   * Test Hive
   *
   * @throws Exception
   */
  @Test
  public void testHiveCreateSelectAgain() throws Exception {
    testHiveCreateSelect();
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
   * Test Hive error
   *
   * @throws Exception
   */
  @Test(expected = SQLException.class)
  public void testHiveError() throws Exception {
    Assert.assertNotEquals(1, processStatement("SOME BAD STATEMENT").size());
  }

  /**
   * Test DFS mkdir and file touch
   *
   * @throws IOException
   */
  @Test
  public void testDfsMkDir() throws Exception {
    Assert.assertFalse(getFileSystem().exists(new Path(getPathString("/some_dir/some_file"))));
    Assert.assertTrue(getFileSystem().mkdirs(new Path(getPathString("/some_dir"))));
    Assert.assertTrue(getFileSystem().createNewFile(new Path(getPathString("/some_dir/some_file"))));
    Assert.assertTrue(getFileSystem().exists(new Path(getPathString("/some_dir/some_file"))));
  }

  /**
   * Test DFS is clean
   *
   * @throws IOException
   */
  @Test
  public void testDfsClean() throws Exception {
    Assert.assertFalse(getFileSystem().exists(new Path(getPathString("/some_dir/some_file"))));
  }

  /**
   * Test DFS path generation
   *
   * @throws Exception
   */
  @Test
  public void testDfsGetPath() throws Exception {
    Assert.assertEquals("/", getPathString(""));
    Assert.assertEquals("/", getPathString("/"));
    Assert.assertEquals("/", getPathString("//"));
    Assert.assertEquals("/tmp", getPathString("tmp"));
    Assert.assertEquals("/tmp", getPathString("/tmp"));
    Assert.assertEquals("/tmp", getPathString("//tmp"));
    Assert.assertEquals("/tmp", getPathString("///tmp"));
    Assert.assertEquals("/tmp//tmp", getPathString("///tmp//tmp"));
  }

  /**
   * Test DFS path URI generation
   *
   * @throws Exception
   */
  @Test
  public void testDfsGetPathUri() throws Exception {
    Assert.assertTrue(getPathUri("").matches("hdfs://localhost:[0-9]+/"));
    Assert.assertTrue(getPathUri("/").matches("hdfs://localhost:[0-9]+/"));
    Assert.assertTrue(getPathUri("//").matches("hdfs://localhost:[0-9]+/"));
    Assert.assertTrue(getPathUri("tmp").matches("hdfs://localhost:[0-9]+/tmp"));
    Assert.assertTrue(getPathUri("/tmp").matches("hdfs://localhost:[0-9]+/tmp"));
    Assert.assertTrue(getPathUri("//tmp").matches("hdfs://localhost:[0-9]+/tmp"));
    Assert.assertTrue(getPathUri("///tmp").matches("hdfs://localhost:[0-9]+/tmp"));
    Assert.assertTrue(getPathUri("///tmp//tmp").matches("hdfs://localhost:[0-9]+/tmp/tmp"));
  }

}
