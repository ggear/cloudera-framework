package com.cloudera.example.table;

import java.util.Arrays;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cloudera.example.Constants;
import com.cloudera.example.TestConstants;
import com.cloudera.example.model.RecordPartition;
import com.cloudera.example.partition.Partition;
import com.cloudera.example.process.Process;
import com.cloudera.example.stage.Stage;
import com.cloudera.framework.main.common.Driver;
import com.cloudera.framework.main.test.MiniClusterDfsMrHiveTest;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

/**
 * Test dataset tables
 */
@RunWith(Parameterized.class)
public class TableTest extends MiniClusterDfsMrHiveTest implements TestConstants {

  /**
   * Paramaterise the unit tests
   */
  @Parameters
  public static Iterable<Object[]> parameters() {
    return Arrays.asList(new Object[][] {
        // All datasets, all subsets
        {
            // All datasets metadata
            new String[] { DS_DIR, DS_DIR, }, //
            new String[] { DIR_DS_MYDATASET_RAW_CANONICAL_TEXT_XML, DIR_DS_MYDATASET_RAW_CANONICAL_TEXT_CSV, }, //
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
        }, // Table DDL parameters and row count tests
            new Map[] {
                // Table mydataset_partitioned_canonical_avro_binary_none
                ImmutableMap.builder().//
                    put(DDL_VAR_FILE, DDL_FILE_CREATE_AVRO). //
                    put(DDL_VAR_NAME, getTableName(DIR_DS_MYDATASET_PARTITIONED_CANONICAL_AVRO)). //
                    put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.RECORD_DDL_YEAR_MONTH)). //
                    put(DDL_VAR_SERDE, AvroSerDe.class.getName()). //
                    put(DDL_VAR_INPUT, AvroContainerInputFormat.class.getName()). //
                    put(DDL_VAR_OUTPUT, AvroContainerOutputFormat.class.getName()). //
                    put(DDL_VAR_LOCATION, DIR_DS_MYDATASET_PARTITIONED_CANONICAL_AVRO). //
                    put(DDL_VAR_ROWS, "332").//
                    build(), //
                // Table mydataset_partitioned_duplicate_avro_binary_none
                ImmutableMap.builder().//
                    put(DDL_VAR_FILE, DDL_FILE_CREATE_AVRO). //
                    put(DDL_VAR_NAME, getTableName(DIR_DS_MYDATASET_PARTITIONED_DUPLICATE_AVRO)). //
                    put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.RECORD_DDL_YEAR_MONTH)). //
                    put(DDL_VAR_SERDE, AvroSerDe.class.getName()). //
                    put(DDL_VAR_INPUT, AvroContainerInputFormat.class.getName()). //
                    put(DDL_VAR_OUTPUT, AvroContainerOutputFormat.class.getName()). //
                    put(DDL_VAR_LOCATION, DIR_DS_MYDATASET_PARTITIONED_DUPLICATE_AVRO). //
                    put(DDL_VAR_ROWS, "140").//
                    build(), //
                // Table mydataset_processed_canonical_parquet_dict_snappy
                ImmutableMap.builder().//
                    put(DDL_VAR_FILE, DDL_FILE_CREATE_PARQUET). //
                    put(DDL_VAR_NAME, getTableName(DIR_DS_MYDATASET_PROCESSED_CANONICAL_PARQUET)). //
                    put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.RECORD_DDL_YEAR_MONTH)). //
                    put(DDL_VAR_SERDE, ParquetHiveSerDe.class.getName()). //
                    put(DDL_VAR_INPUT, MapredParquetInputFormat.class.getName()). //
                    put(DDL_VAR_OUTPUT, MapredParquetOutputFormat.class.getName()). //
                    put(DDL_VAR_LOCATION, DIR_DS_MYDATASET_PROCESSED_CANONICAL_PARQUET). //
                    put(DDL_VAR_ROWS, "332").//
                    build(), //
                // Table mydataset_processed_duplicate_parquet_dict_snappy
                ImmutableMap.builder().//
                    put(DDL_VAR_FILE, DDL_FILE_CREATE_PARQUET). //
                    put(DDL_VAR_NAME, getTableName(DIR_DS_MYDATASET_PROCESSED_DUPLICATE_PARQUET)). //
                    put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.RECORD_DDL_YEAR_MONTH)). //
                    put(DDL_VAR_SERDE, ParquetHiveSerDe.class.getName()). //
                    put(DDL_VAR_INPUT, MapredParquetInputFormat.class.getName()). //
                    put(DDL_VAR_OUTPUT, MapredParquetOutputFormat.class.getName()). //
                    put(DDL_VAR_LOCATION, DIR_DS_MYDATASET_PROCESSED_DUPLICATE_PARQUET). //
                    put(DDL_VAR_ROWS, "0").//
                    build(), //
        }, //
        }, //
    });
  }

  /**
   * Test dataset tables
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testTable() throws Exception {
    for (int i = 0; i < metadata.length; i++) {
      Assert.assertNotNull(processStatement(DDL_DIR, (String) metadata[i].get(DDL_VAR_FILE),
          ImmutableMap.<String, String> builder().putAll(metadata[i]).put(DDL_VAR_SCHEMA, MODEL_AVRO).build()));
      Assert.assertNotNull(processStatement("DESCRIBE " + (String) metadata[i].get(DDL_VAR_NAME)));
      Assert.assertEquals(metadata[i].get(DDL_VAR_ROWS),
          processStatement("SELECT COUNT(1) AS number_of_records FROM " + (String) metadata[i].get(DDL_VAR_NAME))
              .get(0));
    }
  }

  private static String getTableName(String location) {
    return location.substring(1, location.length()).replace('-', '_').replace('/', '_');
  }

  public TableTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets, String[][][] labels,
      @SuppressWarnings("rawtypes") Map[] metadata) {
    super(sources, destinations, datasets, subsets, labels, metadata);
  }

  private static String MODEL_AVRO = new Scanner(TableTest.class.getResourceAsStream(Constants.MODEL_AVRO_FILE),
      Charsets.UTF_8.name()).useDelimiter("\\A").next();

  /**
   * Setup the data
   */
  @Before
  public void setupData() throws Exception {
    Assert.assertEquals(Driver.RETURN_SUCCESS, new Stage(getConf())
        .runner(new String[] { getPathDfs(DIR_DS_MYDATASET_RAW_CANONICAL), getPathDfs(DIR_DS_MYDATASET_STAGED) }));
    Assert.assertEquals(Driver.RETURN_SUCCESS, new Partition(getConf()).runner(
        new String[] { getPathDfs(DIR_DS_MYDATASET_STAGED_CANONICAL), getPathDfs(DIR_DS_MYDATASET_PARTITIONED) }));
    Assert.assertEquals(Driver.RETURN_SUCCESS, new Process(getConf()).runner(
        new String[] { getPathDfs(DIR_DS_MYDATASET_PARTITIONED_CANONICAL), getPathDfs(DIR_DS_MYDATASET_PROCESSED) }));
  }

}
