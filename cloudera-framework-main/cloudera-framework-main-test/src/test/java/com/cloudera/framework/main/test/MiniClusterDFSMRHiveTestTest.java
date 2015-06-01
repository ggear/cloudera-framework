package com.cloudera.framework.main.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

public class MiniClusterDFSMRHiveTestTest extends MiniClusterDFSMRHiveTest {

  @Test
  public void testFileSystemClean() throws IOException {
    Assert.assertFalse(getFileSystem().isDirectory(new Path("/some_dir")));
  }

  @Test
  public void testFileSystemMkDir() throws IOException {
    Assert.assertFalse(getFileSystem().isDirectory(new Path("/some_dir")));
    Assert.assertTrue(getFileSystem().mkdirs(new Path("/some_dir")));
    Assert.assertTrue(getFileSystem().isDirectory(new Path("/some_dir")));
  }

  @Test
  public void testHive() throws Exception {
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
    processStatement("DROP TABLE somedata");
  }

}
