package com.cloudera.framework.common.util;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.framework.common.util.DfsUtil;
import com.cloudera.framework.testing.LocalClusterDfsMrTest;

public class DfsUtilTest extends LocalClusterDfsMrTest {

  @Test
  public void testCanDoAction() throws IllegalArgumentException, IOException {
    Assert.assertTrue(DfsUtil.canDoAction(getFileSystem(), "me", new String[] { "me" }, new Path(getPathString("/tmp")),
        FsAction.READ));
    Assert.assertTrue(DfsUtil.canDoAction(getFileSystem(), "me", new String[] { "me" }, new Path(getPathString("/tmp")),
        FsAction.WRITE));
    Assert.assertFalse(DfsUtil.canDoAction(getFileSystem(), "me", new String[] { "me" },
        new Path(getPathString("/tmp/t")), FsAction.READ));
    Assert.assertFalse(DfsUtil.canDoAction(getFileSystem(), "me", new String[] { "me" },
        new Path(getPathString("/tmp/t")), FsAction.WRITE));
  }

  @Test
  public void testListFiles() throws FileNotFoundException, IllegalArgumentException, IOException {
    Assert.assertEquals(0, DfsUtil.listFiles(getFileSystem(), new Path(getPathString("/tmq/t")), false, false).size());
    Assert.assertEquals(0, DfsUtil.listFiles(getFileSystem(), new Path(getPathString("/tmq/t")), false, false).size());
    Assert.assertEquals(0, DfsUtil.listFiles(getFileSystem(), new Path(getPathString("/tmq/t")), true, false).size());
    Assert.assertEquals(0, DfsUtil.listFiles(getFileSystem(), new Path(getPathString("/tmq/t")), true, true).size());
    Assert.assertEquals(0, DfsUtil.listFiles(getFileSystem(), new Path(getPathString("/tmp/t")), false, false).size());
    Assert.assertEquals(0, DfsUtil.listFiles(getFileSystem(), new Path(getPathString("/tmp/t")), false, false).size());
    Assert.assertEquals(11, DfsUtil.listFiles(getFileSystem(), new Path(getPathString("/tmp/t")), true, false).size());
    Assert.assertEquals(8, DfsUtil.listFiles(getFileSystem(), new Path(getPathString("/tmp/t")), true, true).size());
    Assert.assertEquals(1,
        DfsUtil.listFiles(getFileSystem(), new Path(getPathString("/tmp/t/t1")), false, false).size());
    Assert.assertEquals(1,
        DfsUtil.listFiles(getFileSystem(), new Path(getPathString("/tmp/t/t1")), true, false).size());
    Assert.assertEquals(1, DfsUtil.listFiles(getFileSystem(), new Path(getPathString("/tmp/t/t1")), true, true).size());
    Assert.assertEquals(1,
        DfsUtil.listFiles(getFileSystem(), new Path(getPathString("/tmp/t/t3")), false, false).size());
    Assert.assertEquals(1,
        DfsUtil.listFiles(getFileSystem(), new Path(getPathString("/tmp/t/t3")), true, false).size());
    Assert.assertEquals(0, DfsUtil.listFiles(getFileSystem(), new Path(getPathString("/tmp/t/t3")), true, true).size());
    Assert.assertEquals(2,
        DfsUtil.listFiles(getFileSystem(), new Path(getPathString("/tmp/t/t6")), false, false).size());
    Assert.assertEquals(2,
        DfsUtil.listFiles(getFileSystem(), new Path(getPathString("/tmp/t/t6")), true, false).size());
    Assert.assertEquals(1, DfsUtil.listFiles(getFileSystem(), new Path(getPathString("/tmp/t/t6")), true, true).size());
  }

  @Test
  public void testListDirs() throws FileNotFoundException, IllegalArgumentException, IOException {
    Assert.assertEquals(0, DfsUtil.listDirs(getFileSystem(), new Path(getPathString("/tmq/t")), false, false).size());
    Assert.assertEquals(0, DfsUtil.listDirs(getFileSystem(), new Path(getPathString("/tmq/t")), false, false).size());
    Assert.assertEquals(0, DfsUtil.listDirs(getFileSystem(), new Path(getPathString("/tmq/t")), true, false).size());
    Assert.assertEquals(0, DfsUtil.listDirs(getFileSystem(), new Path(getPathString("/tmq/t")), true, true).size());
    Assert.assertEquals(0, DfsUtil.listDirs(getFileSystem(), new Path(getPathString("/tmp/t")), false, false).size());
    Assert.assertEquals(0, DfsUtil.listDirs(getFileSystem(), new Path(getPathString("/tmp/t")), false, false).size());
    Assert.assertEquals(7, DfsUtil.listDirs(getFileSystem(), new Path(getPathString("/tmp/t")), true, false).size());
    Assert.assertEquals(3, DfsUtil.listDirs(getFileSystem(), new Path(getPathString("/tmp/t")), true, true).size());
    Assert.assertEquals(1,
        DfsUtil.listDirs(getFileSystem(), new Path(getPathString("/tmp/t/t1")), false, false).size());
    Assert.assertEquals(1, DfsUtil.listDirs(getFileSystem(), new Path(getPathString("/tmp/t/t1")), true, false).size());
    Assert.assertEquals(1, DfsUtil.listDirs(getFileSystem(), new Path(getPathString("/tmp/t/t1")), true, true).size());
    Assert.assertEquals(1,
        DfsUtil.listDirs(getFileSystem(), new Path(getPathString("/tmp/t/t3")), false, false).size());
    Assert.assertEquals(1, DfsUtil.listDirs(getFileSystem(), new Path(getPathString("/tmp/t/t3")), true, false).size());
    Assert.assertEquals(0, DfsUtil.listDirs(getFileSystem(), new Path(getPathString("/tmp/t/t3")), true, true).size());
    Assert.assertEquals(1,
        DfsUtil.listDirs(getFileSystem(), new Path(getPathString("/tmp/t/t6")), false, false).size());
    Assert.assertEquals(1, DfsUtil.listDirs(getFileSystem(), new Path(getPathString("/tmp/t/t6")), true, false).size());
    Assert.assertEquals(0, DfsUtil.listDirs(getFileSystem(), new Path(getPathString("/tmp/t/t6")), true, true).size());
  }

  @Before
  public void setUpData() throws IllegalArgumentException, IOException {
    getFileSystem().mkdirs(new Path(getPathString("/tmp/t")),
        new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
    getFileSystem().createNewFile(new Path(getPathString("/tmp/t/t1/t1.txt")));
    getFileSystem().createNewFile(new Path(getPathString("/tmp/t/t2/t2.txt")));
    getFileSystem().createNewFile(new Path(getPathString("/tmp/t/t3/_t3.txt")));
    getFileSystem().createNewFile(new Path(getPathString("/tmp/t/_t4/t4_1.txt")));
    getFileSystem().createNewFile(new Path(getPathString("/tmp/t/_t4/t4_2.txt")));
    getFileSystem().createNewFile(new Path(getPathString("/tmp/t/t5/t5_1.txt")));
    getFileSystem().createNewFile(new Path(getPathString("/tmp/t/t5/_t5_2.txt")));
    getFileSystem().createNewFile(new Path(getPathString("/tmp/t/t6/t6_1.txt")));
    getFileSystem().createNewFile(new Path(getPathString("/tmp/t/t6/_SUCCESS")));
    getFileSystem().createNewFile(new Path(getPathString("/tmp/t/t7/t7_1.txt")));
    getFileSystem().createNewFile(new Path(getPathString("/tmp/t/t7/t7_2.txt")));
  }

}
