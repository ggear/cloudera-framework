package com.cloudera.example.table;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.RegexSerDe;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.mapred.TextInputFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cloudera.example.Constants;
import com.cloudera.example.TestConstants;
import com.cloudera.example.model.RecordPartition;
import com.cloudera.example.model.input.hive.RecordSequenceInputFormatCsv;
import com.cloudera.example.model.input.hive.RecordSequenceInputFormatXml;
import com.cloudera.example.model.input.hive.RecordTextInputFormat;
import com.cloudera.example.model.input.hive.RecordTextInputFormatCsv;
import com.cloudera.example.model.input.hive.RecordTextInputFormatXml;
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
        }, // Table DDL parameters and row count tests
            new Map[] { //
                // Table mydataset_raw_canonical_text_csv_none
                ImmutableMap.builder().//
                    put(DDL_VAR_FILE, DDL_FILE_CREATE_AVRO). //
                    put(DDL_VAR_NAME, getTableName(DIR_ABS_MYDS_RAW_CANONICAL_CSV)). //
                    put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.BATCH_DDL_NAME)). //
                    put(DDL_VAR_SERDE, AvroSerDe.class.getName()). //
                    put(DDL_VAR_INPUT, RecordTextInputFormatCsv.class.getName()). //
                    put(DDL_VAR_OUTPUT, AvroContainerOutputFormat.class.getName()). //
                    put(DDL_VAR_ROOT, DIR_ABS_MYDS_RAW_CANONICAL). //
                    put(DDL_VAR_LOCATION, DIR_ABS_MYDS_RAW_CANONICAL_CSV). //
                    put(DDL_VAR_ROWS, 407). //
                    build(),
                // Table mydataset_raw_canonical_text_xml_none
                ImmutableMap.builder().//
                    put(DDL_VAR_FILE, DDL_FILE_CREATE_AVRO). //
                    put(DDL_VAR_NAME, getTableName(DIR_ABS_MYDS_RAW_CANONICAL_XML)). //
                    put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.BATCH_DDL_NAME)). //
                    put(DDL_VAR_SERDE, AvroSerDe.class.getName()). //
                    put(DDL_VAR_INPUT, RecordTextInputFormatXml.class.getName()). //
                    put(DDL_VAR_OUTPUT, AvroContainerOutputFormat.class.getName()). //
                    put(DDL_VAR_ROOT, DIR_ABS_MYDS_RAW_CANONICAL). //
                    put(DDL_VAR_LOCATION, DIR_ABS_MYDS_RAW_CANONICAL_XML). //
                    put(DDL_VAR_ROWS, 414). //
                    build(),
                // Table mydataset_staged_canonical_sequence_csv_none
                ImmutableMap.builder().//
                    put(DDL_VAR_FILE, DDL_FILE_CREATE_AVRO). //
                    put(DDL_VAR_NAME, getTableName(DIR_ABS_MYDS_STAGED_CANONICAL_CSV)). //
                    put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.BATCH_DDL_ID_START_FINISH)). //
                    put(DDL_VAR_SERDE, AvroSerDe.class.getName()). //
                    put(DDL_VAR_INPUT, RecordSequenceInputFormatCsv.class.getName()). //
                    put(DDL_VAR_OUTPUT, AvroContainerOutputFormat.class.getName()). //
                    put(DDL_VAR_ROOT, DIR_ABS_MYDS_STAGED_CANONICAL). //
                    put(DDL_VAR_LOCATION, DIR_ABS_MYDS_STAGED_CANONICAL_CSV). //
                    put(DDL_VAR_ROWS, 238).//
                    build(), //
                // Table mydataset_staged_canonical_sequence_xml_none
                ImmutableMap.builder().//
                    put(DDL_VAR_FILE, DDL_FILE_CREATE_AVRO). //
                    put(DDL_VAR_NAME, getTableName(DIR_ABS_MYDS_STAGED_CANONICAL_XML)). //
                    put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.BATCH_DDL_ID_START_FINISH)). //
                    put(DDL_VAR_SERDE, AvroSerDe.class.getName()). //
                    put(DDL_VAR_INPUT, RecordSequenceInputFormatXml.class.getName()). //
                    put(DDL_VAR_OUTPUT, AvroContainerOutputFormat.class.getName()). //
                    put(DDL_VAR_ROOT, DIR_ABS_MYDS_STAGED_CANONICAL). //
                    put(DDL_VAR_LOCATION, DIR_ABS_MYDS_STAGED_CANONICAL_XML). //
                    put(DDL_VAR_ROWS, 255).//
                    build(), //
                // TODO: Table mydataset_staged_malformed_text_csv_none
                ImmutableMap.builder().//
                    put(DDL_VAR_FILE, DDL_FILE_CREATE_TEXT). //
                    put(DDL_VAR_NAME, getTableName(DIR_ABS_MYDS_STAGED_MALFORMED_CSV)). //
                    put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.BATCH_DDL_NAME)). //
                    put(DDL_VAR_SERDE, RegexSerDe.class.getName()). //
                    put(DDL_VAR_INPUT, TextInputFormat.class.getName()). //
                    put(DDL_VAR_OUTPUT, HiveIgnoreKeyTextOutputFormat.class.getName()). //
                    put(DDL_VAR_ROOT, DIR_ABS_MYDS_STAGED_CANONICAL). //
                    put(DDL_VAR_LOCATION, DIR_ABS_MYDS_STAGED_MALFORMED_CSV). //
                    put(DDL_VAR_ROWS, 204).//
                    build(), //
                // TODO: Table mydataset_staged_malformed_text_xml_none
                ImmutableMap.builder().//
                    put(DDL_VAR_FILE, DDL_FILE_CREATE_TEXT). //
                    put(DDL_VAR_NAME, getTableName(DIR_ABS_MYDS_STAGED_MALFORMED_XML)). //
                    put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.BATCH_DDL_NAME)). //
                    put(DDL_VAR_SERDE, RegexSerDe.class.getName()). //
                    put(DDL_VAR_INPUT, RecordTextInputFormat.class.getName()). //
                    put(DDL_VAR_OUTPUT, HiveIgnoreKeyTextOutputFormat.class.getName()). //
                    put(DDL_VAR_ROOT, DIR_ABS_MYDS_STAGED_CANONICAL). //
                    put(DDL_VAR_LOCATION, DIR_ABS_MYDS_STAGED_MALFORMED_XML). //
                    put(DDL_VAR_ROWS, 16).//
                    build(), //
                // Table mydataset_partitioned_canonical_avro_binary_none
                ImmutableMap.builder().//
                    put(DDL_VAR_FILE, DDL_FILE_CREATE_AVRO). //
                    put(DDL_VAR_NAME, getTableName(DIR_ABS_MYDS_PARTITIONED_CANONICAL_AVRO)). //
                    put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.RECORD_DDL_YEAR_MONTH)). //
                    put(DDL_VAR_SERDE, AvroSerDe.class.getName()). //
                    put(DDL_VAR_INPUT, AvroContainerInputFormat.class.getName()). //
                    put(DDL_VAR_OUTPUT, AvroContainerOutputFormat.class.getName()). //
                    put(DDL_VAR_ROOT, DIR_ABS_MYDS_PARTITIONED_CANONICAL). //
                    put(DDL_VAR_LOCATION, DIR_ABS_MYDS_PARTITIONED_CANONICAL_AVRO). //
                    put(DDL_VAR_ROWS, 332).//
                    build(), //
                // Table mydataset_partitioned_duplicate_avro_binary_none
                ImmutableMap.builder().//
                    put(DDL_VAR_FILE, DDL_FILE_CREATE_AVRO). //
                    put(DDL_VAR_NAME, getTableName(DIR_ABS_MYDS_PARTITIONED_DUPLICATE_AVRO)). //
                    put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.RECORD_DDL_YEAR_MONTH)). //
                    put(DDL_VAR_SERDE, AvroSerDe.class.getName()). //
                    put(DDL_VAR_INPUT, AvroContainerInputFormat.class.getName()). //
                    put(DDL_VAR_OUTPUT, AvroContainerOutputFormat.class.getName()). //
                    put(DDL_VAR_ROOT, DIR_ABS_MYDS_PARTITIONED_DUPLICATE). //
                    put(DDL_VAR_LOCATION, DIR_ABS_MYDS_PARTITIONED_DUPLICATE_AVRO). //
                    put(DDL_VAR_ROWS, 140).//
                    build(), //
                // TODO: Table mydataset_partitioned_malformed_text_csv_none
                ImmutableMap.builder().//
                    put(DDL_VAR_FILE, DDL_FILE_CREATE_TEXT). //
                    put(DDL_VAR_NAME, getTableName(DIR_ABS_MYDS_PARTITIONED_MALFORMED_CSV)). //
                    put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.BATCH_DDL_NAME)). //
                    put(DDL_VAR_SERDE, RegexSerDe.class.getName()). //
                    put(DDL_VAR_INPUT, TextInputFormat.class.getName()). //
                    put(DDL_VAR_OUTPUT, HiveIgnoreKeyTextOutputFormat.class.getName()). //
                    put(DDL_VAR_ROOT, DIR_ABS_MYDS_STAGED_CANONICAL). //
                    put(DDL_VAR_LOCATION, DIR_ABS_MYDS_PARTITIONED_MALFORMED_CSV). //
                    put(DDL_VAR_ROWS, 17).//
                    build(), //
                // TODO: Table mydataset_partitioned_malformed_text_xml_none
                ImmutableMap.builder().//
                    put(DDL_VAR_FILE, DDL_FILE_CREATE_TEXT). //
                    put(DDL_VAR_NAME, getTableName(DIR_ABS_MYDS_PARTITIONED_MALFORMED_XML)). //
                    put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.BATCH_DDL_NAME)). //
                    put(DDL_VAR_SERDE, RegexSerDe.class.getName()). //
                    put(DDL_VAR_INPUT, RecordTextInputFormat.class.getName()). //
                    put(DDL_VAR_OUTPUT, HiveIgnoreKeyTextOutputFormat.class.getName()). //
                    put(DDL_VAR_ROOT, DIR_ABS_MYDS_STAGED_CANONICAL). //
                    put(DDL_VAR_LOCATION, DIR_ABS_MYDS_PARTITIONED_MALFORMED_XML). //
                    put(DDL_VAR_ROWS, 4).//
                    build(), //
                // Table mydataset_processed_canonical_parquet_dict_snappy
                ImmutableMap.builder().//
                    put(DDL_VAR_FILE, DDL_FILE_CREATE_PARQUET). //
                    put(DDL_VAR_NAME, getTableName(DIR_ABS_MYDS_PROCESSED_CANONICAL_PARQUET)). //
                    put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.RECORD_DDL_YEAR_MONTH)). //
                    put(DDL_VAR_SERDE, ParquetHiveSerDe.class.getName()). //
                    put(DDL_VAR_INPUT, MapredParquetInputFormat.class.getName()). //
                    put(DDL_VAR_OUTPUT, MapredParquetOutputFormat.class.getName()). //
                    put(DDL_VAR_ROOT, DIR_ABS_MYDS_PROCESSED_CANONICAL). //
                    put(DDL_VAR_LOCATION, DIR_ABS_MYDS_PROCESSED_CANONICAL_PARQUET). //
                    put(DDL_VAR_ROWS, 332).//
                    build(), //
                // Table mydataset_processed_rewritten_parquet_dict_snappy
                ImmutableMap.builder().//
                    put(DDL_VAR_FILE, DDL_FILE_CREATE_PARQUET). //
                    put(DDL_VAR_NAME, getTableName(DIR_ABS_MYDS_PROCESSED_REWRITTEN_PARQUET)). //
                    put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.RECORD_DDL_YEAR_MONTH)). //
                    put(DDL_VAR_SERDE, ParquetHiveSerDe.class.getName()). //
                    put(DDL_VAR_INPUT, MapredParquetInputFormat.class.getName()). //
                    put(DDL_VAR_OUTPUT, MapredParquetOutputFormat.class.getName()). //
                    put(DDL_VAR_ROOT, DIR_ABS_MYDS_PROCESSED_REWRITTEN). //
                    put(DDL_VAR_LOCATION, DIR_ABS_MYDS_PROCESSED_REWRITTEN_PARQUET). //
                    put(DDL_VAR_ROWS, 0).//
                    build(), //
                // Table mydataset_processed_duplicate_parquet_dict_snappy
                ImmutableMap.builder().//
                    put(DDL_VAR_FILE, DDL_FILE_CREATE_PARQUET). //
                    put(DDL_VAR_NAME, getTableName(DIR_ABS_MYDS_PROCESSED_DUPLICATE_PARQUET)). //
                    put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.RECORD_DDL_YEAR_MONTH)). //
                    put(DDL_VAR_SERDE, ParquetHiveSerDe.class.getName()). //
                    put(DDL_VAR_INPUT, MapredParquetInputFormat.class.getName()). //
                    put(DDL_VAR_OUTPUT, MapredParquetOutputFormat.class.getName()). //
                    put(DDL_VAR_ROOT, DIR_ABS_MYDS_PROCESSED_DUPLICATE). //
                    put(DDL_VAR_LOCATION, DIR_ABS_MYDS_PROCESSED_DUPLICATE_PARQUET). //
                    put(DDL_VAR_ROWS, 0).//
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
      String testName = "Table index [" + i + "] and name [" + metadata[i].get(DDL_VAR_NAME) + "]";
      Assert.assertNotNull(testName,
          processStatement(DDL_DIR, (String) metadata[i].get(DDL_VAR_FILE),
              ImmutableMap.<String, String> builder().putAll(metadata[i]).put(DDL_VAR_SCHEMA, MODEL_AVRO).build(),
              ImmutableMap.of(Constants.CONFIG_INPUT_PATH,
                  getFileSystem().makeQualified(new Path((String) metadata[i].get(DDL_VAR_ROOT))).toString())));
      Assert.assertNotNull(testName, processStatement("DESCRIBE " + (String) metadata[i].get(DDL_VAR_NAME)));
      Assert.assertEquals(testName, metadata[i].get(DDL_VAR_ROWS),
          processStatement("SELECT * FROM " + (String) metadata[i].get(DDL_VAR_NAME),
              Collections.<String, String> emptyMap(),
              ImmutableMap.of(Constants.CONFIG_INPUT_PATH,
                  getFileSystem().makeQualified(new Path((String) metadata[i].get(DDL_VAR_ROOT))).toString(),
                  HiveConf.ConfVars.HIVEINPUTFORMAT.varname, HiveInputFormat.class.getName()),
              1000).size());
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
        .runner(new String[] { getPathDfs(DIR_ABS_MYDS_RAW_CANONICAL), getPathDfs(DIR_ABS_MYDS_STAGED) }));
    Assert.assertEquals(Driver.RETURN_SUCCESS, new Partition(getConf())
        .runner(new String[] { getPathDfs(DIR_ABS_MYDS_STAGED_CANONICAL), getPathDfs(DIR_ABS_MYDS_PARTITIONED) }));
    Assert.assertEquals(Driver.RETURN_SUCCESS, new Process(getConf())
        .runner(new String[] { getPathDfs(DIR_ABS_MYDS_PARTITIONED_CANONICAL), getPathDfs(DIR_ABS_MYDS_PROCESSED) }));
  }

}
