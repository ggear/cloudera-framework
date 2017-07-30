package com.cloudera.framework.example.one.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.util.Collections;
import java.util.Scanner;

import com.cloudera.framework.common.Driver;
import com.cloudera.framework.common.util.FsUtil;
import com.cloudera.framework.example.one.Constants;
import com.cloudera.framework.example.one.TestBase;
import com.cloudera.framework.example.one.model.RecordPartition;
import com.cloudera.framework.example.one.model.input.hive.RecordSequenceInputFormatCsv;
import com.cloudera.framework.example.one.model.input.hive.RecordSequenceInputFormatXml;
import com.cloudera.framework.example.one.model.input.hive.RecordTextInputFormat;
import com.cloudera.framework.example.one.model.input.hive.RecordTextInputFormatCsv;
import com.cloudera.framework.example.one.model.input.hive.RecordTextInputFormatXml;
import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.HiveServer;
import com.cloudera.framework.testing.server.PythonServer;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.googlecode.zohhak.api.Coercion;
import com.googlecode.zohhak.api.TestWith;
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
import org.junit.ClassRule;
import org.junit.runner.RunWith;

/**
 * Test dataset tables
 */
@RunWith(TestRunner.class)
public class Table extends TestBase {

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static final HiveServer hiveServer = HiveServer.getInstance();

  @ClassRule
  public static final PythonServer pythonServer = PythonServer.getInstance();

  private static final String MODEL_AVRO = new Scanner(Table.class.getResourceAsStream(Constants.MODEL_AVRO_FILE), Charsets.UTF_8.name())
    .useDelimiter("\\A").next();

  public final TestMetaData testMetaDataAll = super.testMetaDataAll //
    .parameters( //
      ImmutableMap.builder(). //
        put(DDL_VAR_FILE, DDL_FILE_CREATE_AVRO). //
        put(DDL_VAR_NAME, getTableName(DIR_ABS_MYDS_RAW_CANONICAL_CSV)). //
        put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.BATCH_DDL_NAME)). //
        put(DDL_VAR_SERDE, AvroSerDe.class.getName()). //
        put(DDL_VAR_INPUT, RecordTextInputFormatCsv.class.getName()). //
        put(DDL_VAR_OUTPUT, AvroContainerOutputFormat.class.getName()). //
        put(DDL_VAR_ROOT, DIR_ABS_MYDS_RAW_CANONICAL). //
        put(DDL_VAR_LOCATION, dfsServer.getPathUri(DIR_ABS_MYDS_RAW_CANONICAL_CSV)). //
        put(DDL_VAR_ROWS, 407). //
        build(),
      ImmutableMap.builder(). //
        put(DDL_VAR_FILE, DDL_FILE_CREATE_AVRO). //
        put(DDL_VAR_NAME, getTableName(DIR_ABS_MYDS_RAW_CANONICAL_XML)). //
        put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.BATCH_DDL_NAME)). //
        put(DDL_VAR_SERDE, AvroSerDe.class.getName()). //
        put(DDL_VAR_INPUT, RecordTextInputFormatXml.class.getName()). //
        put(DDL_VAR_OUTPUT, AvroContainerOutputFormat.class.getName()). //
        put(DDL_VAR_ROOT, DIR_ABS_MYDS_RAW_CANONICAL). //
        put(DDL_VAR_LOCATION, dfsServer.getPathUri(DIR_ABS_MYDS_RAW_CANONICAL_XML)). //
        put(DDL_VAR_ROWS, 414). //
        build(),
      ImmutableMap.builder(). //
        put(DDL_VAR_FILE, DDL_FILE_CREATE_AVRO). //
        put(DDL_VAR_NAME, getTableName(DIR_ABS_MYDS_STAGED_CANONICAL_CSV)). //
        put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.BATCH_DDL_ID_START_FINISH)). //
        put(DDL_VAR_SERDE, AvroSerDe.class.getName()). //
        put(DDL_VAR_INPUT, RecordSequenceInputFormatCsv.class.getName()). //
        put(DDL_VAR_OUTPUT, AvroContainerOutputFormat.class.getName()). //
        put(DDL_VAR_ROOT, DIR_ABS_MYDS_STAGED_CANONICAL). //
        put(DDL_VAR_LOCATION, dfsServer.getPathUri(DIR_ABS_MYDS_STAGED_CANONICAL_CSV)). //
        put(DDL_VAR_ROWS, 238).//
        build(), //
      ImmutableMap.builder(). //
        put(DDL_VAR_FILE, DDL_FILE_CREATE_AVRO). //
        put(DDL_VAR_NAME, getTableName(DIR_ABS_MYDS_STAGED_CANONICAL_XML)). //
        put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.BATCH_DDL_ID_START_FINISH)). //
        put(DDL_VAR_SERDE, AvroSerDe.class.getName()). //
        put(DDL_VAR_INPUT, RecordSequenceInputFormatXml.class.getName()). //
        put(DDL_VAR_OUTPUT, AvroContainerOutputFormat.class.getName()). //
        put(DDL_VAR_ROOT, DIR_ABS_MYDS_STAGED_CANONICAL). //
        put(DDL_VAR_LOCATION, dfsServer.getPathUri(DIR_ABS_MYDS_STAGED_CANONICAL_XML)). //
        put(DDL_VAR_ROWS, 255).//
        build(), //
      ImmutableMap.builder(). //
        put(DDL_VAR_FILE, DDL_FILE_CREATE_TEXT). //
        put(DDL_VAR_NAME, getTableName(DIR_ABS_MYDS_STAGED_MALFORMED_CSV)). //
        put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.BATCH_DDL_NAME)). //
        put(DDL_VAR_SERDE, RegexSerDe.class.getName()). //
        put(DDL_VAR_INPUT, TextInputFormat.class.getName()). //
        put(DDL_VAR_OUTPUT, HiveIgnoreKeyTextOutputFormat.class.getName()). //
        put(DDL_VAR_ROOT, DIR_ABS_MYDS_STAGED_CANONICAL). //
        put(DDL_VAR_LOCATION, dfsServer.getPathUri(DIR_ABS_MYDS_STAGED_MALFORMED_CSV)). //
        put(DDL_VAR_ROWS, 204).//
        build(), //
      ImmutableMap.builder(). //
        put(DDL_VAR_FILE, DDL_FILE_CREATE_TEXT). //
        put(DDL_VAR_NAME, getTableName(DIR_ABS_MYDS_STAGED_MALFORMED_XML)). //
        put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.BATCH_DDL_NAME)). //
        put(DDL_VAR_SERDE, RegexSerDe.class.getName()). //
        put(DDL_VAR_INPUT, RecordTextInputFormat.class.getName()). //
        put(DDL_VAR_OUTPUT, HiveIgnoreKeyTextOutputFormat.class.getName()). //
        put(DDL_VAR_ROOT, DIR_ABS_MYDS_STAGED_CANONICAL). //
        put(DDL_VAR_LOCATION, dfsServer.getPathUri(DIR_ABS_MYDS_STAGED_MALFORMED_XML)). //
        put(DDL_VAR_ROWS, 16).//
        build(), //
      ImmutableMap.builder(). //
        put(DDL_VAR_FILE, DDL_FILE_CREATE_AVRO). //
        put(DDL_VAR_NAME, getTableName(DIR_ABS_MYDS_PARTITIONED_CANONICAL_AVRO)). //
        put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.RECORD_DDL_YEAR_MONTH)). //
        put(DDL_VAR_SERDE, AvroSerDe.class.getName()). //
        put(DDL_VAR_INPUT, AvroContainerInputFormat.class.getName()). //
        put(DDL_VAR_OUTPUT, AvroContainerOutputFormat.class.getName()). //
        put(DDL_VAR_ROOT, DIR_ABS_MYDS_PARTITIONED_CANONICAL). //
        put(DDL_VAR_LOCATION, dfsServer.getPathUri(DIR_ABS_MYDS_PARTITIONED_CANONICAL_AVRO)). //
        put(DDL_VAR_ROWS, 332).//
        build(), //
      ImmutableMap.builder(). //
        put(DDL_VAR_FILE, DDL_FILE_CREATE_AVRO). //
        put(DDL_VAR_NAME, getTableName(DIR_ABS_MYDS_PARTITIONED_DUPLICATE_AVRO)). //
        put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.RECORD_DDL_YEAR_MONTH)). //
        put(DDL_VAR_SERDE, AvroSerDe.class.getName()). //
        put(DDL_VAR_INPUT, AvroContainerInputFormat.class.getName()). //
        put(DDL_VAR_OUTPUT, AvroContainerOutputFormat.class.getName()). //
        put(DDL_VAR_ROOT, DIR_ABS_MYDS_PARTITIONED_DUPLICATE). //
        put(DDL_VAR_LOCATION, dfsServer.getPathUri(DIR_ABS_MYDS_PARTITIONED_DUPLICATE_AVRO)). //
        put(DDL_VAR_ROWS, 140).//
        build(), //
      ImmutableMap.builder(). //
        put(DDL_VAR_FILE, DDL_FILE_CREATE_TEXT). //
        put(DDL_VAR_NAME, getTableName(DIR_ABS_MYDS_PARTITIONED_MALFORMED_CSV)). //
        put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.BATCH_DDL_NAME)). //
        put(DDL_VAR_SERDE, RegexSerDe.class.getName()). //
        put(DDL_VAR_INPUT, TextInputFormat.class.getName()). //
        put(DDL_VAR_OUTPUT, HiveIgnoreKeyTextOutputFormat.class.getName()). //
        put(DDL_VAR_ROOT, DIR_ABS_MYDS_STAGED_CANONICAL). //
        put(DDL_VAR_LOCATION, dfsServer.getPathUri(DIR_ABS_MYDS_PARTITIONED_MALFORMED_CSV)). //
        put(DDL_VAR_ROWS, 17).//
        build(), //
      ImmutableMap.builder(). //
        put(DDL_VAR_FILE, DDL_FILE_CREATE_TEXT). //
        put(DDL_VAR_NAME, getTableName(DIR_ABS_MYDS_PARTITIONED_MALFORMED_XML)). //
        put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.BATCH_DDL_NAME)). //
        put(DDL_VAR_SERDE, RegexSerDe.class.getName()). //
        put(DDL_VAR_INPUT, RecordTextInputFormat.class.getName()). //
        put(DDL_VAR_OUTPUT, HiveIgnoreKeyTextOutputFormat.class.getName()). //
        put(DDL_VAR_ROOT, DIR_ABS_MYDS_STAGED_CANONICAL). //
        put(DDL_VAR_LOCATION, dfsServer.getPathUri(DIR_ABS_MYDS_PARTITIONED_MALFORMED_XML)). //
        put(DDL_VAR_ROWS, 4).//
        build(), //
      ImmutableMap.builder(). //
        put(DDL_VAR_FILE, DDL_FILE_CREATE_PARQUET). //
        put(DDL_VAR_NAME, getTableName(DIR_ABS_MYDS_CLEANSED_CANONICAL_PARQUET)). //
        put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.RECORD_DDL_YEAR_MONTH)). //
        put(DDL_VAR_SERDE, ParquetHiveSerDe.class.getName()). //
        put(DDL_VAR_INPUT, MapredParquetInputFormat.class.getName()). //
        put(DDL_VAR_OUTPUT, MapredParquetOutputFormat.class.getName()). //
        put(DDL_VAR_ROOT, DIR_ABS_MYDS_CLEANSED_CANONICAL). //
        put(DDL_VAR_LOCATION, dfsServer.getPathUri(DIR_ABS_MYDS_CLEANSED_CANONICAL_PARQUET)). //
        put(DDL_VAR_ROWS, 332).//
        build(), //
      ImmutableMap.builder(). //
        put(DDL_VAR_FILE, DDL_FILE_CREATE_PARQUET). //
        put(DDL_VAR_NAME, getTableName(DIR_ABS_MYDS_CLEANSED_REWRITTEN_PARQUET)). //
        put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.RECORD_DDL_YEAR_MONTH)). //
        put(DDL_VAR_SERDE, ParquetHiveSerDe.class.getName()). //
        put(DDL_VAR_INPUT, MapredParquetInputFormat.class.getName()). //
        put(DDL_VAR_OUTPUT, MapredParquetOutputFormat.class.getName()). //
        put(DDL_VAR_ROOT, DIR_ABS_MYDS_CLEANSED_REWRITTEN). //
        put(DDL_VAR_LOCATION, dfsServer.getPathUri(DIR_ABS_MYDS_CLEANSED_REWRITTEN_PARQUET)). //
        put(DDL_VAR_ROWS, 0).//
        build(), //
      ImmutableMap.builder(). //
        put(DDL_VAR_FILE, DDL_FILE_CREATE_PARQUET). //
        put(DDL_VAR_NAME, getTableName(DIR_ABS_MYDS_CLEANSED_DUPLICATE_PARQUET)). //
        put(DDL_VAR_PARTITION, Joiner.on(", ").join(RecordPartition.RECORD_DDL_YEAR_MONTH)). //
        put(DDL_VAR_SERDE, ParquetHiveSerDe.class.getName()). //
        put(DDL_VAR_INPUT, MapredParquetInputFormat.class.getName()). //
        put(DDL_VAR_OUTPUT, MapredParquetOutputFormat.class.getName()). //
        put(DDL_VAR_ROOT, DIR_ABS_MYDS_CLEANSED_DUPLICATE). //
        put(DDL_VAR_LOCATION, dfsServer.getPathUri(DIR_ABS_MYDS_CLEANSED_DUPLICATE_PARQUET)). //
        put(DDL_VAR_ROWS, 0).//
        build());

  private static String getTableName(String location) {
    return location.substring(1, location.length()).replace('-', '_').replace('/', '_');
  }

  /**
   * Test dataset tables
   */
  @SuppressWarnings("unchecked")
  @TestWith({"testMetaDataAll"})
  public void testTable(TestMetaData testMetaData) throws Exception {
    hiveServer.execute("CREATE DATABASE IF NOT EXISTS " + DIR_REL_MYDS + " LOCATION '" + dfsServer.getPathUri(DIR_REL_MYDS) + "'");
    assertEquals(Driver.RETURN_SUCCESS,
      new com.cloudera.framework.example.one.process.Process(dfsServer.getConf())
        .runner(new String[]{dfsServer.getPath(DIR_ABS_MYDS_RAW).toString(), dfsServer.getPath(DIR_ABS_MYDS_STAGED).toString(),
          dfsServer.getPath(DIR_ABS_MYDS_PARTITIONED).toString(), dfsServer.getPath(DIR_ABS_MYDS_CLEANSED).toString()}));
    for (int i = 0; i < testMetaData.getParameters().length; i++) {
      String testName = "Table index [" + i + "], name [" + testMetaData.getParameters()[i].get(DDL_VAR_NAME) + "]";
      String inputPath = dfsServer.getFileSystem().makeQualified(new Path((String) testMetaData.getParameters()[i].get(DDL_VAR_ROOT)))
        .toString();
      assertNotNull(testName,
        hiveServer.execute(
          new File(DDL_DIR, (String) testMetaData.getParameters()[i].get(DDL_VAR_FILE)), ImmutableMap.<String, String>builder()
            .putAll(testMetaData.getParameters()[i]).put(DDL_VAR_SCHEMA, MODEL_AVRO).put(DDL_VAR_DATABASE, DIR_REL_MYDS).build(),
          ImmutableMap.of(Constants.CONFIG_INPUT_PATH, inputPath)));
      assertNotNull(testName, hiveServer.execute("DESCRIBE " + testMetaData.getParameters()[i].get(DDL_VAR_NAME)));
      assertEquals(testName, testMetaData.getParameters()[i].get(DDL_VAR_ROWS),
        hiveServer.execute("SELECT * FROM " + testMetaData.getParameters()[i].get(DDL_VAR_NAME),
          Collections.emptyMap(), ImmutableMap.of(Constants.CONFIG_INPUT_PATH, inputPath,
            HiveConf.ConfVars.HIVEINPUTFORMAT.varname, HiveInputFormat.class.getName()),
          1000).size());
    }
    for (File script : FsUtil.listFiles(ABS_DIR_HIVE_QUERY)) {
      hiveServer.execute(script);
    }
    for (File script : FsUtil.listFiles(ABS_DIR_PYTHON)) {
      pythonServer.execute(script);
    }
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
