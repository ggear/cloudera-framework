package com.cloudera.framework.main.test;

import java.io.File;

import org.junit.Assert;
import org.junit.Test;

public class BaseTestTest {

  @Test
  public void testPathHDFS() {
    Assert.assertEquals(BaseTest.PATH_HDFS_LOCAL, BaseTest.getPathHDFS(""));
    Assert.assertEquals(BaseTest.PATH_HDFS_LOCAL, BaseTest.getPathHDFS("/"));
    Assert.assertEquals(BaseTest.PATH_HDFS_LOCAL, BaseTest.getPathHDFS("//"));
    Assert.assertEquals(BaseTest.PATH_HDFS_LOCAL + "/tmp",
        BaseTest.getPathHDFS("tmp"));
    Assert.assertEquals(BaseTest.PATH_HDFS_LOCAL + "/tmp",
        BaseTest.getPathHDFS("/tmp"));
    Assert.assertEquals(BaseTest.PATH_HDFS_LOCAL + "/tmp",
        BaseTest.getPathHDFS("//tmp"));
    Assert.assertEquals(BaseTest.PATH_HDFS_LOCAL + "/tmp",
        BaseTest.getPathHDFS("///tmp"));
    Assert.assertEquals(BaseTest.PATH_HDFS_LOCAL + "/tmp/tmp",
        BaseTest.getPathHDFS("///tmp//tmp"));
  }

  @Test
  public void testPathLocal() {
    String localDir = new File(".").getAbsolutePath();
    localDir = localDir.substring(0, localDir.length() - 2);
    Assert.assertEquals(localDir, BaseTest.getPathLocal(""));
    Assert.assertEquals(localDir, BaseTest.getPathLocal("/"));
    Assert.assertEquals(localDir, BaseTest.getPathLocal("//"));
    Assert.assertEquals(localDir + "/tmp", BaseTest.getPathLocal("tmp"));
    Assert.assertEquals(localDir + "/tmp", BaseTest.getPathLocal("/tmp"));
    Assert.assertEquals(localDir + "/tmp", BaseTest.getPathLocal("//tmp"));
    Assert.assertEquals(localDir + "/tmp", BaseTest.getPathLocal("///tmp"));
    Assert.assertEquals(localDir + "/tmp/tmp",
        BaseTest.getPathLocal("///tmp//tmp"));
  }

}
