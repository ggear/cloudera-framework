package com.cloudera.example.table;

import java.util.Arrays;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cloudera.example.ConstantsTest;
import com.cloudera.example.process.Cleanse;
import com.cloudera.framework.main.common.Driver;
import com.cloudera.framework.main.test.MiniClusterDfsMrHiveTest;
import com.google.common.collect.ImmutableMap;

/**
 * Test dataset tables
 */
@RunWith(Parameterized.class)
public class TableTest extends MiniClusterDfsMrHiveTest implements ConstantsTest {

  @Parameters
  public static Iterable<Object[]> paramaters() {
    return Arrays.asList(new Object[][] {
        // All datasets
        {
            // Both tab and comma dataset metadata
            new String[] { DS_DIR, DS_DIR, }, //
            new String[] { DIR_DS_MYDATASET_RAW_SOURCE_TEXT_TAB, DIR_DS_MYDATASET_RAW_SOURCE_TEXT_COMMA, }, //
            new String[] { DS_MYDATASET, DS_MYDATASET }, //
            new String[][] {
                // Both tab and comma dataset
                { DSS_MYDATASET_TAB }, //
                { DSS_MYDATASET_COMMA }, //
        }, // All tab and comma dataset subsets
            new String[][][] {
                //
                { { null }, }, //
                { { null }, }, //
        }, // Counter equality tests
            new Map[] {}, //
        }, //
    });
  }

  public TableTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets, String[][][] labels,
      @SuppressWarnings("rawtypes") Map[] counters) {
    super(sources, destinations, datasets, subsets, labels, counters);
  }

  /**
   * Setup the data
   */
  @Before
  public void setupData() throws Exception {
    Assert.assertEquals(Driver.RETURN_SUCCESS, new Cleanse(getConf())
        .runner(new String[] { getPathDfs(DIR_DS_MYDATASET_RAW), getPathDfs(DIR_DS_MYDATASET_PROCESSED) }));
  }

  private static final String[] TABLE_DDL_FILE = { DDL_TEXT, DDL_TEXT, DDL_AVRO, DDL_AVRO };
  private static final String[] TABLE_DDL_DELIM = { "\\t", ",", "", "" };
  private static final String[] TABLE_DFS_LOCATIONS = { DIR_DS_MYDATASET_RAW_SOURCE_TEXT_TAB,
      DIR_DS_MYDATASET_RAW_SOURCE_TEXT_COMMA, DIR_DS_MYDATASET_PROCESSED_CLEANSED,
      DIR_DS_MYDATASET_PROCESSED_DUPLICATE };

  /**
   * Test the tables
   */
  @Test
  public void testTable() throws Exception {
    String tableAvroSchema = IOUtils.toString(TableTest.class.getResourceAsStream(AVRO_DIR + "/" + AVRO_MODEL));
    for (int i = 0; i < TABLE_DDL_FILE.length; i++) {
      String tableName = TABLE_DFS_LOCATIONS[i].substring(1, TABLE_DFS_LOCATIONS[i].length()).replace('-', '_')
          .replace('/', '_');
      processStatement(DDL_DIR, TABLE_DDL_FILE[i], ImmutableMap.of(DDL_TABLENAME, tableName, DDL_TABLEDELIM,
          TABLE_DDL_DELIM[i], DDL_TABLEAVROSCHEMA, tableAvroSchema, DDL_TABLELOCATION, TABLE_DFS_LOCATIONS[i]));
      processStatement(SQL_DIR, SQL_STATS, ImmutableMap.of(DDL_TABLENAME, tableName));
    }
  }

}
