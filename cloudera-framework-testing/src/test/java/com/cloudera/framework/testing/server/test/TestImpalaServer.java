package com.cloudera.framework.testing.server.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.ImpalaServer;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

// TODO: Enable test once ImpalaServer implementation is complete
@Ignore
@RunWith(TestRunner.class)
public class TestImpalaServer implements TestConstants {

  @ClassRule
  public static final ImpalaServer impalaServer = ImpalaServer.getInstance();

  @Test
  public void testImplicitDependencies() {
    assertTrue(DfsServer.getInstance().isStarted());
  }

  @Test(expected = IOException.class)
  public void testImpalaNullFile() throws Exception {
    impalaServer.execute((File) null);
  }

  @Test(expected = IOException.class)
  public void testImpalaNoFile() throws Exception {
    impalaServer.execute(new File("/sql/some-non-existent-script.sql"));
  }

  /**
   * Test Hive
   */
  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void testImpalaCreateSelect() throws Exception {
    new File(REL_DIR_DATA).mkdirs();
    File localDataFile = new File(REL_DIR_DATA + "/somedata.csv");
    FileUtils.writeStringToFile(localDataFile, "1,1\n2,2\n3,3\n");
    assertEquals(0, DfsServer.getInstance().listFilesDfs("/usr/hive", true).length);
    assertEquals(0, impalaServer.execute("SHOW TABLES").size());
    assertEquals(1, impalaServer.execute("SHOW DATABASES").size());
    assertEquals(2,
      impalaServer
        .execute(new File(getClass().getResource("/ddl/create.sql").getPath()),
          new ImmutableMap.Builder<String, String>().put("test.table.name", "somedata").put("test.table.field.delim", ",").build())
        .size());
    assertEquals(0,
      impalaServer.execute("LOAD DATA LOCAL INPATH '" + localDataFile.toString() + "' OVERWRITE INTO TABLE somedata").size());
    assertEquals(1, DfsServer.getInstance().listFilesDfs("/usr/hive", true).length);
    assertEquals("3", impalaServer.execute("SELECT count(1) AS cnt FROM somedata").get(0));
    assertEquals("2", impalaServer.execute("SELECT col1 FROM somedata WHERE col2 = 2").get(0));
    assertEquals(2, impalaServer
      .execute("SELECT * FROM somedata", Collections.emptyMap(), Collections.emptyMap(), 2).size());
    assertEquals(1, impalaServer.execute("SHOW TABLES").size());
    assertEquals(0, impalaServer.execute("CREATE DATABASE somedb").size());
    assertEquals(0, impalaServer.execute("USE somedb").size());
    assertEquals(0, impalaServer
      .execute("CREATE TABLE somedata (col1 INT, col2 INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE").size());
    assertEquals(0,
      impalaServer
        .execute("CREATE TABLE somedata_copy (col1 INT, col2 INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
        .size());
    assertEquals(2, impalaServer.execute("SHOW TABLES").size());
    assertEquals(0, impalaServer.execute("USE default").size());
    assertEquals(1, impalaServer.execute("SHOW TABLES").size());
    assertEquals(2, impalaServer.execute("SHOW DATABASES").size());
  }

  @Test
  public void testImpalaCreateSelectAgain() throws Exception {
    testImpalaCreateSelect();
  }

}
