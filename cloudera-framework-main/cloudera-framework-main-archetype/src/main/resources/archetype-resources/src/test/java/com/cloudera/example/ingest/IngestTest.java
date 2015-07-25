package com.cloudera.example.ingest;

import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cloudera.example.TestConstants;
import com.cloudera.framework.main.test.LocalClusterDfsMrTest;

/**
 * Test dataset ingest
 */
@RunWith(Parameterized.class)
public class IngestTest extends LocalClusterDfsMrTest implements TestConstants {

  /**
   * Paramaterise the unit tests
   */
  @Parameters
  public static Iterable<Object[]> parameters() {
    return Arrays.asList(new Object[][] {
        //
        {
            //
            new String[] { DS_DIR, DS_DIR, }, //
            new String[] { DIR_DS_MYDATASET_RAW_SOURCE_TEXT_TAB, DIR_DS_MYDATASET_RAW_SOURCE_TEXT_COMMA, }, //
            new String[] { DS_MYDATASET, DS_MYDATASET }, //
            new String[][] {
                //
                { null }, //
                { null }, //
        }, //
            new String[][][] {
                //
                { { null }, }, //
                { { null }, }, //
        }, //
            new Map[] {}, //
        }, //
    });
  }

  @Test
  public void testIngest() throws Exception {
    Assert.assertTrue(getFileSystem().exists(new Path(getPathDfs(DIR_DS_MYDATASET_RAW_SOURCE_TEXT_TAB))));
    Assert.assertTrue(
        getFileSystem().listFiles(new Path(getPathDfs(DIR_DS_MYDATASET_RAW_SOURCE_TEXT_TAB)), true).hasNext());
    Assert.assertTrue(getFileSystem().exists(new Path(getPathDfs(DIR_DS_MYDATASET_RAW_SOURCE_TEXT_COMMA))));
    Assert.assertTrue(
        getFileSystem().listFiles(new Path(getPathDfs(DIR_DS_MYDATASET_RAW_SOURCE_TEXT_COMMA)), true).hasNext());
  }

  public IngestTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets, String[][][] labels,
      @SuppressWarnings("rawtypes") Map[] metadata) {
    super(sources, destinations, datasets, subsets, labels, metadata);
  }

}
