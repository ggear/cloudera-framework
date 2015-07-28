package com.cloudera.example.stage;

import java.util.Arrays;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cloudera.example.TestConstants;
import com.cloudera.example.model.RecordCounter;
import com.cloudera.framework.main.common.Driver;
import com.cloudera.framework.main.test.LocalClusterDfsMrTest;
import com.google.common.collect.ImmutableMap;

/**
 * Test dataset process
 */
@RunWith(Parameterized.class)
public class StageTest extends LocalClusterDfsMrTest implements TestConstants {

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
            new Map[] {
                //
                ImmutableMap.of(Process.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 72L, //
                        RecordCounter.RECORDS_CLEANSED, 10L, //
                        RecordCounter.RECORDS_DUPLICATE, 30L, //
                        RecordCounter.RECORDS_MALFORMED, 32L//
            )), //
        }, //
        }, //
        // Pristine datasets
        {
            // Both tab and comma dataset metadata
            new String[] { DS_DIR, DS_DIR, }, //
            new String[] { DIR_DS_MYDATASET_RAW_SOURCE_TEXT_TSV, DIR_DS_MYDATASET_RAW_SOURCE_TEXT_CSV, }, //
            new String[] { DS_MYDATASET, DS_MYDATASET }, //
            new String[][] {
                // Both tab and comma dataset
                { DSS_MYDATASET_TSV }, //
                { DSS_MYDATASET_CSV }, //
        }, // Pristine tab and comma dataset subsets
            new String[][][] {
                //
                { { DSS_MYDATASET_PRISTINE_SINGLE }, }, //
                { { DSS_MYDATASET_PRISTINE_SINGLE }, }, //
        }, // Counter equality tests
            new Map[] {
                //
                ImmutableMap.of(Process.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 20L, //
                        RecordCounter.RECORDS_CLEANSED, 10L, //
                        RecordCounter.RECORDS_DUPLICATE, 10L, //
                        RecordCounter.RECORDS_MALFORMED, 0L//
            )), //
        }, //
        }, //
    });
  }

  /**
   * Test dataset process
   */
  @Test
  public void testProcess() throws Exception {
    Driver driver = new Stage(getConf());
    Assert.assertEquals(Driver.RETURN_SUCCESS,
        driver.runner(new String[] { getPathDfs(DIR_DS_MYDATASET_RAW), getPathDfs(DIR_DS_MYDATASET_PROCESSED) }));

    // TODO Re-enable counter checking
    // assertCounterEquals(metadata[0], driver.getCounters());
  }

  public StageTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets, String[][][] labels,
      @SuppressWarnings("rawtypes") Map[] metadata) {
    super(sources, destinations, datasets, subsets, labels, metadata);
  }

}
