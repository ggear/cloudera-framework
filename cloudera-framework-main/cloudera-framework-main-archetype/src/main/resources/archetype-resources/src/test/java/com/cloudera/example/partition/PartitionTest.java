package com.cloudera.example.partition;

import java.util.Arrays;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cloudera.example.TestConstants;
import com.cloudera.example.model.RecordCounter;
import com.cloudera.example.stage.Stage;
import com.cloudera.framework.main.common.Driver;
import com.cloudera.framework.main.test.LocalClusterDfsMrTest;
import com.google.common.collect.ImmutableMap;

/**
 * Test dataset partition
 */
@RunWith(Parameterized.class)
public class PartitionTest extends LocalClusterDfsMrTest implements TestConstants {

  /**
   * Paramaterise the unit tests
   */
  @Parameters
  public static Iterable<Object[]> parameters() {
    return Arrays.asList(new Object[][] {
        // CSV dataset, pristine subset
        {
            // CSV dataset metadata
            new String[] { DS_DIR, }, //
            new String[] { DIR_ABS_MYDS_RAW_CANONICAL_CSV, }, //
            new String[] { DS_MYDATASET, }, //
            new String[][] {
                // CSV dataset
                { DSS_MYDATASET_CSV }, //
        }, // CSV pristine-single dataset subset
            new String[][][] {
                //
                { { DSS_MYDATASET_PRISTINE_SINGLE }, }, //
        }, // Counter equality tests
            new Map[] {
                // First run
                ImmutableMap.of(Partition.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 1L, //
                        RecordCounter.RECORDS_CANONICAL, 1L, //
                        RecordCounter.RECORDS_DUPLICATE, 0L, //
                        RecordCounter.RECORDS_MALFORMED, 0L//
            )), //
                // Second run
                ImmutableMap.of(Partition.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 0L, //
                        RecordCounter.RECORDS_CANONICAL, 0L, //
                        RecordCounter.RECORDS_DUPLICATE, 0L, //
                        RecordCounter.RECORDS_MALFORMED, 0L//
            )), //
        }, //
        }, //
        // XML dataset, pristine subset
        {
            // XML dataset metadata
            new String[] { DS_DIR, }, //
            new String[] { DIR_ABS_MYDS_RAW_CANONICAL_XML, }, //
            new String[] { DS_MYDATASET, }, //
            new String[][] {
                // XML dataset
                { DSS_MYDATASET_XML }, //
        }, // XML pristine-single dataset subset
            new String[][][] {
                //
                { { DSS_MYDATASET_PRISTINE_SINGLE }, }, //
        }, // Counter equality tests
            new Map[] {
                // First run
                ImmutableMap.of(Partition.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 1L, //
                        RecordCounter.RECORDS_CANONICAL, 1L, //
                        RecordCounter.RECORDS_DUPLICATE, 0L, //
                        RecordCounter.RECORDS_MALFORMED, 0L//
            )), //
                // Second run
                ImmutableMap.of(Partition.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 0L, //
                        RecordCounter.RECORDS_CANONICAL, 0L, //
                        RecordCounter.RECORDS_DUPLICATE, 0L, //
                        RecordCounter.RECORDS_MALFORMED, 0L//
            )), //
        }, //
        }, //
        // All datasets, all subsets
        {
            // All datasets metadata
            new String[] { DS_DIR, DS_DIR, }, //
            new String[] { DIR_ABS_MYDS_RAW_CANONICAL_XML, DIR_ABS_MYDS_RAW_CANONICAL_CSV, }, //
            new String[] { DS_MYDATASET, DS_MYDATASET }, //
            new String[][] {
                // All datasets
                { DSS_MYDATASET_XML }, //
                { DSS_MYDATASET_CSV }, //
        }, // All dataset subsets
            new String[][][] {
                //
                { { null }, }, //
                { { null }, }, //
        }, // Counter equality tests
            new Map[] {
                // First run
                ImmutableMap.of(Partition.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 493L, //
                        RecordCounter.RECORDS_CANONICAL, 332L, //
                        RecordCounter.RECORDS_DUPLICATE, 140L, //
                        RecordCounter.RECORDS_MALFORMED, 21L//
            )), //
                // Second run
                ImmutableMap.of(Partition.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 0L, //
                        RecordCounter.RECORDS_CANONICAL, 0L, //
                        RecordCounter.RECORDS_DUPLICATE, 0L, //
                        RecordCounter.RECORDS_MALFORMED, 0L//
            )), //
        }, //
        }, //
    });
  }

  /**
   * Test dataset partition
   */
  @Test
  public void testPartition() throws Exception {
    Driver driver = new Partition(getConf());
    Assert.assertEquals(Driver.RETURN_SUCCESS, driver
        .runner(new String[] { getPathDfs(DIR_ABS_MYDS_STAGED_CANONICAL), getPathDfs(DIR_ABS_MYDS_PARTITIONED) }));
    assertCounterEquals(metadata[0], driver.getCounters());
    Assert.assertEquals(Driver.RETURN_SUCCESS, driver
        .runner(new String[] { getPathDfs(DIR_ABS_MYDS_STAGED_CANONICAL), getPathDfs(DIR_ABS_MYDS_PARTITIONED) }));
    assertCounterEquals(metadata[1], driver.getCounters());
  }

  public PartitionTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets,
      String[][][] labels, @SuppressWarnings("rawtypes") Map[] metadata) {
    super(sources, destinations, datasets, subsets, labels, metadata);
  }

  /**
   * Setup the data
   */
  @Before
  public void setupData() throws Exception {
    Assert.assertEquals(Driver.RETURN_SUCCESS, new Stage(getConf())
        .runner(new String[] { getPathDfs(DIR_ABS_MYDS_RAW_CANONICAL), getPathDfs(DIR_ABS_MYDS_STAGED) }));
  }

}
