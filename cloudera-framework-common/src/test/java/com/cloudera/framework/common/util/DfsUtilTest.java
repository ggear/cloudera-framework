package com.cloudera.framework.common.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;

import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(TestRunner.class)
public class DfsUtilTest {

  @ClassRule
  public static DfsServer dfsServer = DfsServer.getInstance();

  @Test
  public void testCanDoAction() throws IllegalArgumentException, IOException {
    assertTrue(
      DfsUtil.canDoAction(dfsServer.getFileSystem(), "me", new String[]{"me"}, new Path(dfsServer.getPathUri("/tmp")), FsAction.READ));
    assertTrue(DfsUtil.canDoAction(dfsServer.getFileSystem(), "me", new String[]{"me"}, new Path(dfsServer.getPathUri("/tmp")),
      FsAction.WRITE));
    assertFalse(DfsUtil.canDoAction(dfsServer.getFileSystem(), "me", new String[]{"me"}, new Path(dfsServer.getPathUri("/tmp/t")),
      FsAction.READ));
    assertFalse(DfsUtil.canDoAction(dfsServer.getFileSystem(), "me", new String[]{"me"}, new Path(dfsServer.getPathUri("/tmp/t")),
      FsAction.WRITE));
  }

  @Test
  public void testListFiles() throws FileNotFoundException, IllegalArgumentException, IOException {
    assertEquals(0, DfsUtil.listFiles(dfsServer.getFileSystem(), dfsServer.getPath("/tmq/t"), false, false).size());
    assertEquals(0, DfsUtil.listFiles(dfsServer.getFileSystem(), dfsServer.getPath("/tmq/t"), false, false).size());
    assertEquals(0, DfsUtil.listFiles(dfsServer.getFileSystem(), dfsServer.getPath("/tmq/t"), true, false).size());
    assertEquals(0, DfsUtil.listFiles(dfsServer.getFileSystem(), dfsServer.getPath("/tmq/t"), true, true).size());
    assertEquals(0, DfsUtil.listFiles(dfsServer.getFileSystem(), dfsServer.getPath("/tmp/t"), false, false).size());
    assertEquals(0, DfsUtil.listFiles(dfsServer.getFileSystem(), dfsServer.getPath("/tmp/t"), false, false).size());
    assertEquals(11, DfsUtil.listFiles(dfsServer.getFileSystem(), dfsServer.getPath("/tmp/t"), true, false).size());
    assertEquals(8, DfsUtil.listFiles(dfsServer.getFileSystem(), dfsServer.getPath("/tmp/t"), true, true).size());
    assertEquals(1, DfsUtil.listFiles(dfsServer.getFileSystem(), dfsServer.getPath("/tmp/t/t1"), false, false).size());
    assertEquals(1, DfsUtil.listFiles(dfsServer.getFileSystem(), dfsServer.getPath("/tmp/t/t1"), true, false).size());
    assertEquals(1, DfsUtil.listFiles(dfsServer.getFileSystem(), dfsServer.getPath("/tmp/t/t1"), true, true).size());
    assertEquals(1, DfsUtil.listFiles(dfsServer.getFileSystem(), dfsServer.getPath("/tmp/t/t3"), false, false).size());
    assertEquals(1, DfsUtil.listFiles(dfsServer.getFileSystem(), dfsServer.getPath("/tmp/t/t3"), true, false).size());
    assertEquals(0, DfsUtil.listFiles(dfsServer.getFileSystem(), dfsServer.getPath("/tmp/t/t3"), true, true).size());
    assertEquals(2, DfsUtil.listFiles(dfsServer.getFileSystem(), dfsServer.getPath("/tmp/t/t6"), false, false).size());
    assertEquals(2, DfsUtil.listFiles(dfsServer.getFileSystem(), dfsServer.getPath("/tmp/t/t6"), true, false).size());
    assertEquals(1, DfsUtil.listFiles(dfsServer.getFileSystem(), dfsServer.getPath("/tmp/t/t6"), true, true).size());
  }

  @Test
  public void testListDirs() throws FileNotFoundException, IllegalArgumentException, IOException {
    assertEquals(0, DfsUtil.listDirs(dfsServer.getFileSystem(), dfsServer.getPath("/tmq/t"), false, false).size());
    assertEquals(0, DfsUtil.listDirs(dfsServer.getFileSystem(), dfsServer.getPath("/tmq/t"), false, false).size());
    assertEquals(0, DfsUtil.listDirs(dfsServer.getFileSystem(), dfsServer.getPath("/tmq/t"), true, false).size());
    assertEquals(0, DfsUtil.listDirs(dfsServer.getFileSystem(), dfsServer.getPath("/tmq/t"), true, true).size());
    assertEquals(0, DfsUtil.listDirs(dfsServer.getFileSystem(), dfsServer.getPath("/tmp/t"), false, false).size());
    assertEquals(0, DfsUtil.listDirs(dfsServer.getFileSystem(), dfsServer.getPath("/tmp/t"), false, false).size());
    assertEquals(7, DfsUtil.listDirs(dfsServer.getFileSystem(), dfsServer.getPath("/tmp/t"), true, false).size());
    assertEquals(3, DfsUtil.listDirs(dfsServer.getFileSystem(), dfsServer.getPath("/tmp/t"), true, true).size());
    assertEquals(1, DfsUtil.listDirs(dfsServer.getFileSystem(), dfsServer.getPath("/tmp/t/t1"), false, false).size());
    assertEquals(1, DfsUtil.listDirs(dfsServer.getFileSystem(), dfsServer.getPath("/tmp/t/t1"), true, false).size());
    assertEquals(1, DfsUtil.listDirs(dfsServer.getFileSystem(), dfsServer.getPath("/tmp/t/t1"), true, true).size());
    assertEquals(1, DfsUtil.listDirs(dfsServer.getFileSystem(), dfsServer.getPath("/tmp/t/t3"), false, false).size());
    assertEquals(1, DfsUtil.listDirs(dfsServer.getFileSystem(), dfsServer.getPath("/tmp/t/t3"), true, false).size());
    assertEquals(0, DfsUtil.listDirs(dfsServer.getFileSystem(), dfsServer.getPath("/tmp/t/t3"), true, true).size());
    assertEquals(1, DfsUtil.listDirs(dfsServer.getFileSystem(), dfsServer.getPath("/tmp/t/t6"), false, false).size());
    assertEquals(1, DfsUtil.listDirs(dfsServer.getFileSystem(), dfsServer.getPath("/tmp/t/t6"), true, false).size());
    assertEquals(0, DfsUtil.listDirs(dfsServer.getFileSystem(), dfsServer.getPath("/tmp/t/t6"), true, true).size());
  }

  @Before
  public void setUpData() throws IllegalArgumentException, IOException {
    dfsServer.getFileSystem().mkdirs(dfsServer.getPath("/tmp/t"), new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
    dfsServer.getFileSystem().createNewFile(dfsServer.getPath("/tmp/t/t1/t1.txt"));
    dfsServer.getFileSystem().createNewFile(dfsServer.getPath("/tmp/t/t2/t2.txt"));
    dfsServer.getFileSystem().createNewFile(dfsServer.getPath("/tmp/t/t3/_t3.txt"));
    dfsServer.getFileSystem().createNewFile(dfsServer.getPath("/tmp/t/_t4/t4_1.txt"));
    dfsServer.getFileSystem().createNewFile(dfsServer.getPath("/tmp/t/_t4/t4_2.txt"));
    dfsServer.getFileSystem().createNewFile(dfsServer.getPath("/tmp/t/t5/t5_1.txt"));
    dfsServer.getFileSystem().createNewFile(dfsServer.getPath("/tmp/t/t5/_t5_2.txt"));
    dfsServer.getFileSystem().createNewFile(dfsServer.getPath("/tmp/t/t6/t6_1.txt"));
    dfsServer.getFileSystem().createNewFile(dfsServer.getPath("/tmp/t/t6/_SUCCESS"));
    dfsServer.getFileSystem().createNewFile(dfsServer.getPath("/tmp/t/t7/t7_1.txt"));
    dfsServer.getFileSystem().createNewFile(dfsServer.getPath("/tmp/t/t7/t7_2.txt"));
  }

}
