package com.cloudera.example.ingest;

import java.util.Arrays;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cloudera.example.TestConstants;
import com.cloudera.example.model.RecordCounter;
import com.cloudera.framework.common.Driver;
import com.cloudera.framework.testing.LocalClusterDfsMrTest;
import com.google.common.collect.ImmutableMap;

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
                ImmutableMap.of(//
                    Stage.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.FILES, 1L, //
                        RecordCounter.FILES_CANONICAL, 1L, //
                        RecordCounter.FILES_DUPLICATE, 0L, //
                        RecordCounter.FILES_MALFORMED, 0L //
            ), //
                    Partition.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 1L, //
                        RecordCounter.RECORDS_CANONICAL, 1L, //
                        RecordCounter.RECORDS_DUPLICATE, 0L, //
                        RecordCounter.RECORDS_MALFORMED, 0L//
            ), //
                    Process.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 1L, //
                        RecordCounter.RECORDS_CANONICAL, 1L, //
                        RecordCounter.RECORDS_DUPLICATE, 0L, //
                        RecordCounter.RECORDS_MALFORMED, 0L//
            )//
            ), //
               // Second run
                ImmutableMap.of(//
                    Stage.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.FILES, 0L, //
                        RecordCounter.FILES_CANONICAL, 0L, //
                        RecordCounter.FILES_DUPLICATE, 0L, //
                        RecordCounter.FILES_MALFORMED, 0L //
            ), //
                    Partition.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 0L, //
                        RecordCounter.RECORDS_CANONICAL, 0L, //
                        RecordCounter.RECORDS_DUPLICATE, 0L, //
                        RecordCounter.RECORDS_MALFORMED, 0L//
            ), //
                    Process.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 0L, //
                        RecordCounter.RECORDS_CANONICAL, 0L, //
                        RecordCounter.RECORDS_DUPLICATE, 0L, //
                        RecordCounter.RECORDS_MALFORMED, 0L//
            )//
            ), //
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
                ImmutableMap.of(//
                    Stage.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.FILES, 1L, //
                        RecordCounter.FILES_CANONICAL, 1L, //
                        RecordCounter.FILES_DUPLICATE, 0L, //
                        RecordCounter.FILES_MALFORMED, 0L //
            ), //
                    Partition.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 1L, //
                        RecordCounter.RECORDS_CANONICAL, 1L, //
                        RecordCounter.RECORDS_DUPLICATE, 0L, //
                        RecordCounter.RECORDS_MALFORMED, 0L//
            ), //
                    Process.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 1L, //
                        RecordCounter.RECORDS_CANONICAL, 1L, //
                        RecordCounter.RECORDS_DUPLICATE, 0L, //
                        RecordCounter.RECORDS_MALFORMED, 0L//
            )//
            ), //
               // Second run
                ImmutableMap.of(//
                    Stage.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.FILES, 0L, //
                        RecordCounter.FILES_CANONICAL, 0L, //
                        RecordCounter.FILES_DUPLICATE, 0L, //
                        RecordCounter.FILES_MALFORMED, 0L //
            ), //
                    Partition.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 0L, //
                        RecordCounter.RECORDS_CANONICAL, 0L, //
                        RecordCounter.RECORDS_DUPLICATE, 0L, //
                        RecordCounter.RECORDS_MALFORMED, 0L//
            ), //
                    Process.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 0L, //
                        RecordCounter.RECORDS_CANONICAL, 0L, //
                        RecordCounter.RECORDS_DUPLICATE, 0L, //
                        RecordCounter.RECORDS_MALFORMED, 0L//
            )//
            ), //
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
                ImmutableMap.of(//
                    Stage.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.FILES, 93L, //
                        RecordCounter.FILES_CANONICAL, 56L, //
                        RecordCounter.FILES_DUPLICATE, 0L, //
                        RecordCounter.FILES_MALFORMED, 37L //
            ), //
                    Partition.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 493L, //
                        RecordCounter.RECORDS_CANONICAL, 332L, //
                        RecordCounter.RECORDS_DUPLICATE, 140L, //
                        RecordCounter.RECORDS_MALFORMED, 21L//
            ), //
                    Process.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 332L, //
                        RecordCounter.RECORDS_CANONICAL, 332L, //
                        RecordCounter.RECORDS_DUPLICATE, 0L, //
                        RecordCounter.RECORDS_MALFORMED, 0L//
            )//
            ), //
               // Second run
                ImmutableMap.of(//
                    Stage.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.FILES, 0L, //
                        RecordCounter.FILES_CANONICAL, 0L, //
                        RecordCounter.FILES_DUPLICATE, 0L, //
                        RecordCounter.FILES_MALFORMED, 0L //
            ), //
                    Partition.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 0L, //
                        RecordCounter.RECORDS_CANONICAL, 0L, //
                        RecordCounter.RECORDS_DUPLICATE, 0L, //
                        RecordCounter.RECORDS_MALFORMED, 0L//
            ), //
                    Process.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 0L, //
                        RecordCounter.RECORDS_CANONICAL, 0L, //
                        RecordCounter.RECORDS_DUPLICATE, 0L, //
                        RecordCounter.RECORDS_MALFORMED, 0L//
            )//
            ), //
        }, //
        }, //
    });
  }

  /**
   * Test dataset process
   */
  @Test
  public void testProcess() throws Exception {
    Driver driver = new Ingest(getConf());
    Assert.assertEquals(Driver.RETURN_SUCCESS,
        driver.runner(new String[] { getPathString(DIR_ABS_MYDS_RAW), getPathString(DIR_ABS_MYDS_STAGED),
            getPathString(DIR_ABS_MYDS_PARTITIONED), getPathString(DIR_ABS_MYDS_PROCESSED) }));
    assertCounterEquals(metadata[0], driver.getCounters());
    Assert.assertEquals(Driver.RETURN_SUCCESS,
        driver.runner(new String[] { getPathString(DIR_ABS_MYDS_RAW), getPathString(DIR_ABS_MYDS_STAGED),
            getPathString(DIR_ABS_MYDS_PARTITIONED), getPathString(DIR_ABS_MYDS_PROCESSED) }));
    assertCounterEquals(metadata[1], driver.getCounters());
  }

  public IngestTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets, String[][][] labels,
      @SuppressWarnings("rawtypes") Map[] metadata) {
    super(sources, destinations, datasets, subsets, labels, metadata);
  }

}
