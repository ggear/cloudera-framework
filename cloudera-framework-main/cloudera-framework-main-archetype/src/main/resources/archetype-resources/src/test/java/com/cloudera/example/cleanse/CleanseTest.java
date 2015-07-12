package com.cloudera.example.cleanse;

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
  public static Iterable<Object[]> paramaters() {
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
        new Map[] {
            //
            ImmutableMap.of(Cleanse.class.getCanonicalName(), ImmutableMap.of(Counter.RECORDS_CLEANSED, 0L,
                Counter.RECORDS_DUPLICATE, 0L, Counter.RECORDS_MALFORMED, 0L)), //
            ImmutableMap.of(Cleanse.class.getCanonicalName(), ImmutableMap.of(Counter.RECORDS, Long.MAX_VALUE)), //
            ImmutableMap.of(Cleanse.class.getCanonicalName(), ImmutableMap.of(Counter.RECORDS, 0L)), //
        }, //
    }, //
    });
  }

  public CleanseTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets,
      String[][][] labels, @SuppressWarnings("rawtypes") Map[] counters) {
    super(sources, destinations, datasets, subsets, labels, counters);
  }

  @Test
  public void testCleanse() throws Exception {
    Driver driver = new Cleanse(getConf());
    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        driver.runner(new String[] { getPathDfs(DIR_DS_MYDATASET_RAW_SOURCE_TEXT_TAB),
            getPathDfs(DIR_DS_MYDATASET_PROCESSED) }));
    assertCounterEqualsLessThanGreaterThan(counters[0], counters[1], counters[2], driver.getCounters());
  }

}
