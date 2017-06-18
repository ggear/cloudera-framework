package com.cloudera.framework.testing.server.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.HiveServer;
import com.cloudera.framework.testing.server.MrServer;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

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

  @Test(expected = IOException.class)
  public void testHiveNullFile() throws Exception {
    getHiveServer().execute((File) null);
  }

  @Test(expected = IOException.class)
  public void testHiveNoFile() throws Exception {
    getHiveServer().execute(new File("/hive/some-non-existent-script.sql"));
  }

  /**
   * Test Hive
   */
  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void testHiveCreateSelect() throws Exception {
    new File(REL_DIR_DATA).mkdirs();
    File localDataFile = new File(REL_DIR_DATA + "/somedata.csv");
    FileUtils.writeStringToFile(localDataFile, "1,1\n2,2\n3,3\n");
    assertEquals(0, DfsServer.getInstance().listFilesDfs("/usr/hive").length);
    assertEquals(0, getHiveServer().execute("SHOW TABLES").size());
    assertEquals(1, getHiveServer().execute("SHOW DATABASES").size());
    assertEquals(2,
      getHiveServer()
        .execute(new File(getClass().getResource("/ddl/create.sql").getPath()),
          new ImmutableMap.Builder<String, String>().put("test.table.name", "somedata").put("test.table.field.delim", ",").build())
        .size());
    assertEquals(0,
      getHiveServer().execute("LOAD DATA LOCAL INPATH '" + localDataFile.toString() + "' OVERWRITE INTO TABLE somedata").size());
    assertEquals(1, DfsServer.getInstance().listFilesDfs("/usr/hive").length);
    assertEquals("3", getHiveServer().execute("SELECT count(1) AS cnt FROM somedata").get(0));
    assertEquals("2", getHiveServer().execute("SELECT col1 FROM somedata WHERE col2 = 2").get(0));
    assertEquals(2, getHiveServer()
      .execute("SELECT * FROM somedata", Collections.emptyMap(), Collections.emptyMap(), 2).size());
    assertEquals(1, getHiveServer().execute("SHOW TABLES").size());
    assertEquals(0, getHiveServer().execute("CREATE DATABASE somedb").size());
    assertEquals(0, getHiveServer().execute("USE somedb").size());
    assertEquals(0, getHiveServer()
      .execute("CREATE TABLE somedata (col1 INT, col2 INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE").size());
    assertEquals(0,
      getHiveServer()
        .execute("CREATE TABLE somedata_copy (col1 INT, col2 INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
        .size());
    assertEquals(2, getHiveServer().execute("SHOW TABLES").size());
    assertEquals(0, getHiveServer().execute("USE default").size());
    assertEquals(1, getHiveServer().execute("SHOW TABLES").size());
    assertEquals(2, getHiveServer().execute("SHOW DATABASES").size());
  }

  @Test
  public void testHiveCreateSelectAgain() throws Exception {
    testHiveCreateSelect();
  }

}
