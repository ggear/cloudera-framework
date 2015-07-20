package com.cloudera.example.process;

import java.util.Arrays;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cloudera.example.ConstantsTest;
import com.cloudera.framework.main.common.Driver;
import com.cloudera.framework.main.test.LocalClusterDfsMrTest;
import com.google.common.collect.ImmutableMap;

/**
 * Test dataset cleanse
 */
@RunWith(Parameterized.class)
public class CleanseTest extends LocalClusterDfsMrTest implements ConstantsTest {

  @Parameters
  public static Iterable<Object[]> parameters() {
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
            new Map[] {
                //
                ImmutableMap.of(Cleanse.class.getCanonicalName(),
                    ImmutableMap.of(//
                        Counter.RECORDS, 72L, //
                        Counter.RECORDS_CLEANSED, 10L, //
                        Counter.RECORDS_DUPLICATE, 30L, //
                        Counter.RECORDS_MALFORMED, 32L//
            )), //
        }, //
        }, //
        // Pristine datasets
        {
            // Both tab and comma dataset metadata
            new String[] { DS_DIR, DS_DIR, }, //
            new String[] { DIR_DS_MYDATASET_RAW_SOURCE_TEXT_TAB, DIR_DS_MYDATASET_RAW_SOURCE_TEXT_COMMA, }, //
            new String[] { DS_MYDATASET, DS_MYDATASET }, //
            new String[][] {
                // Both tab and comma dataset
                { DSS_MYDATASET_TAB }, //
                { DSS_MYDATASET_COMMA }, //
        }, // Pristine tab and comma dataset subsets
            new String[][][] {
                //
                { { DSS_MYDATASET_PRISTINE }, }, //
                { { DSS_MYDATASET_PRISTINE }, }, //
        }, // Counter equality tests
            new Map[] {
                //
                ImmutableMap.of(Cleanse.class.getCanonicalName(),
                    ImmutableMap.of(//
                        Counter.RECORDS, 20L, //
                        Counter.RECORDS_CLEANSED, 10L, //
                        Counter.RECORDS_DUPLICATE, 10L, //
                        Counter.RECORDS_MALFORMED, 0L//
            )), //
        }, //
        }, //
    });
  }

  public CleanseTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets,
      String[][][] labels, @SuppressWarnings("rawtypes") Map[] metadata) {
    super(sources, destinations, datasets, subsets, labels, metadata);
  }

  /**
   * Test cleanse
   */
  @Test
  public void testCleanse() throws Exception {
    Driver driver = new Cleanse(getConf());
    Assert.assertEquals(Driver.RETURN_SUCCESS,
        driver.runner(new String[] { getPathDfs(DIR_DS_MYDATASET_RAW), getPathDfs(DIR_DS_MYDATASET_PROCESSED) }));
    assertCounterEquals(metadata[0], driver.getCounters());
  }

}
