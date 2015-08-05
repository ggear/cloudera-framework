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
        // Single dataset, pristine subset
        {
            // Both tab and comma dataset metadata
            new String[] { DS_DIR, }, //
            new String[] { DIR_DS_MYDATASET_RAW_SOURCE_TEXT_TSV, }, //
            new String[] { DS_MYDATASET, }, //
            new String[][] {
                // Both tab and comma dataset
                { DSS_MYDATASET_TSV }, //
        }, // Pristine tab and comma dataset subsets
            new String[][][] {
                //
                { { DSS_MYDATASET_PRISTINE_SINGLE }, }, //
        }, // Counter equality tests
            new Map[] {
                //
                ImmutableMap.of(Stage.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.FILES, 1L, //
                        RecordCounter.FILES_PARTITIONED, 1L, //
                        RecordCounter.FILES_MALFORMED, 0L //
            )), //
        }, //
        }, //
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
                ImmutableMap.of(Stage.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.FILES, 92L, //
                        RecordCounter.FILES_PARTITIONED, 58L, //
                        RecordCounter.FILES_MALFORMED, 34L //
            )), //
        }, //
        }, //
    });
  }

  /**
   * Test stage
   */
  @Test
  public void testStage() throws Exception {
    Driver driver = new Stage(getConf());
    Assert.assertEquals(Driver.RETURN_SUCCESS,
        driver.runner(new String[] { getPathDfs(DIR_DS_MYDATASET_RAW_SOURCE), getPathDfs(DIR_DS_MYDATASET_STAGED) }));
    assertCounterEquals(metadata[0], driver.getCounters());
  }

  public StageTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets, String[][][] labels,
      @SuppressWarnings("rawtypes") Map[] metadata) {
    super(sources, destinations, datasets, subsets, labels, metadata);
  }

}
