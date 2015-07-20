package com.cloudera.example.table;

import java.io.IOException;
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
  public static Iterable<Object[]> parameters() {
    try {
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
          }, // Table DDL parameters and row count tests
              new Map[] {
                  // Raw text tab
                  ImmutableMap.of(//
                      DDL_FILE, DDL_FILE_TEXT, //
                      DDL_TABLEDELIM, "\\t", //
                      DDL_TABLELOCATION, DIR_DS_MYDATASET_RAW_SOURCE_TEXT_TAB, //
                      DDL_ROWS, "36" //
              ), // Raw text comma
                  ImmutableMap.of(//
                      DDL_FILE, DDL_FILE_TEXT, //
                      DDL_TABLEDELIM, ",", //
                      DDL_TABLELOCATION, DIR_DS_MYDATASET_RAW_SOURCE_TEXT_COMMA, //
                      DDL_ROWS, "36" //
              ), // Processed cleansed avro
                  ImmutableMap.of(//
                      DDL_FILE, DDL_FILE_AVRO, //
                      DDL_TABLEAVROSCHEMA, IOUtils.toString(TableTest.class.getResourceAsStream(MODEL_AVRO)), //
                      DDL_TABLELOCATION, DIR_DS_MYDATASET_PROCESSED_CLEANSED_AVRO, //
                      DDL_ROWS, "10" //
              ), // Processed duplicate avro
                  ImmutableMap.of(//
                      DDL_FILE, DDL_FILE_AVRO, //
                      DDL_TABLEAVROSCHEMA, IOUtils.toString(TableTest.class.getResourceAsStream(MODEL_AVRO)), //
                      DDL_TABLELOCATION, DIR_DS_MYDATASET_PROCESSED_DUPLICATE_AVRO, //
                      DDL_ROWS, "30" //
              ), //
          }, //
          }, //
      });
    } catch (IOException exception) {
      throw new RuntimeException("Could not set up parameters", exception);
    }
  }

  public TableTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets, String[][][] labels,
      @SuppressWarnings("rawtypes") Map[] metadata) {
    super(sources, destinations, datasets, subsets, labels, metadata);
  }

  /**
   * Setup the data
   */
  @Before
  public void setupData() throws Exception {
    Assert.assertEquals(Driver.RETURN_SUCCESS, new Cleanse(getConf())
        .runner(new String[] { getPathDfs(DIR_DS_MYDATASET_RAW), getPathDfs(DIR_DS_MYDATASET_PROCESSED) }));
  }

  /**
   * Test the tables
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testTable() throws Exception {
    for (int i = 0; i < metadata.length; i++) {
      String tableName = ((String) metadata[i].get(DDL_TABLELOCATION))
          .substring(1, ((String) metadata[i].get(DDL_TABLELOCATION)).length()).replace('-', '_').replace('/', '_');
      Assert.assertNotNull(processStatement(DDL_DIR, (String) metadata[i].get(DDL_FILE),
          ImmutableMap.<String, String> builder().putAll(metadata[i]).put(DDL_TABLENAME, tableName).build()));
      Assert.assertEquals(metadata[i].get(DDL_ROWS),
          processStatement("SELECT COUNT(1) AS number_of_records FROM " + tableName).get(0));
    }
  }

}
