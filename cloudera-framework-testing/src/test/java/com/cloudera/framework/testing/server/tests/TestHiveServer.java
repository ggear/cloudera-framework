package com.cloudera.framework.testing.server.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Collections;

import org.junit.Test;

import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.HiveServer;
import com.cloudera.framework.testing.server.MrServer;
import com.google.common.collect.ImmutableMap;

public abstract class TestHiveServer implements TestConstants {

  public abstract HiveServer getHiveServer();

  @Test
  public void testGetHiveServer() {
    assertNotNull(getHiveServer());
    assertTrue(getHiveServer().isStarted());
  }

  @Test
  public void testImplicitDependencies() {
    assertTrue(DfsServer.getInstance().isStarted());
    assertTrue(MrServer.getInstance().isStarted());
  }

  /**
   * Test Hive
   *
   * @throws Exception
   */
  @Test
  public void testHiveCreateSelect() throws Exception {
    new File(REL_DIR_DATA).mkdirs();
    File localDataFile = new File(REL_DIR_DATA + "/somedata.csv");
    BufferedWriter writer = new BufferedWriter(new FileWriter(localDataFile));
    writer.write("1,1\n");
    writer.write("2,2\n");
    writer.write("3,3\n");
    writer.close();
    assertEquals(0, DfsServer.getInstance().listFilesDfs("/usr/hive").length);
    assertEquals(0, getHiveServer().execute("SHOW TABLES").size());
    assertEquals(2,
        getHiveServer()
            .execute("/ddl", "create.sql",
                new ImmutableMap.Builder<String, String>().put("test.table.name", "somedata").put("test.table.field.delim", ",").build())
            .size());
    assertEquals(0,
        getHiveServer().execute("LOAD DATA LOCAL INPATH '" + localDataFile.toString() + "' OVERWRITE INTO TABLE somedata").size());
    assertEquals(1, DfsServer.getInstance().listFilesDfs("/usr/hive").length);
    assertEquals("3", getHiveServer().execute("SELECT count(1) AS cnt FROM somedata").get(0));
    assertEquals("2", getHiveServer().execute("SELECT col1 FROM somedata WHERE col2 = 2").get(0));
    assertEquals(2, getHiveServer()
        .execute("SELECT * FROM somedata", Collections.<String, String> emptyMap(), Collections.<String, String> emptyMap(), 2).size());
    assertEquals(1, getHiveServer().execute("SHOW TABLES").size());
  }

  @Test
  public void testHiveCreateSelectAgain() throws Exception {
    testHiveCreateSelect();
  }

}
