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
        // All datasets
        {
            // Both tab and comma dataset metadata
            new String[] { DS_DIR, DS_DIR, }, //
            new String[] { DIR_DS_MYDATASET_RAW_SOURCE_TEXT_TSV, DIR_DS_MYDATASET_RAW_SOURCE_TEXT_CSV, }, //
            new String[] { DS_MYDATASET, DS_MYDATASET }, //
            new String[][] {
                // Both tab and comma dataset
                { DSS_MYDATASET_TSV }, //
                { DSS_MYDATASET_CSV }, //
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

  /**
   * Test dataset ingest
   */
  @Test
  public void testIngest() throws Exception {
    Assert.assertTrue(getFileSystem().exists(new Path(getPathDfs(DIR_DS_MYDATASET_RAW_SOURCE_TEXT_TSV))));
    Assert.assertTrue(
        getFileSystem().listFiles(new Path(getPathDfs(DIR_DS_MYDATASET_RAW_SOURCE_TEXT_TSV)), true).hasNext());
    Assert.assertTrue(getFileSystem().exists(new Path(getPathDfs(DIR_DS_MYDATASET_RAW_SOURCE_TEXT_CSV))));
    Assert.assertTrue(
        getFileSystem().listFiles(new Path(getPathDfs(DIR_DS_MYDATASET_RAW_SOURCE_TEXT_CSV)), true).hasNext());
  }

  public IngestTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets, String[][][] labels,
      @SuppressWarnings("rawtypes") Map[] metadata) {
    super(sources, destinations, datasets, subsets, labels, metadata);
  }

}
