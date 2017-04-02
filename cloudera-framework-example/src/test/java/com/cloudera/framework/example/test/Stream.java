package com.cloudera.framework.example.test;

import static com.cloudera.framework.example.stream.Stream.PROPERTY_BATCH_SIZE;
import static com.cloudera.framework.example.stream.Stream.PROPERTY_POLL_MS;
import static com.cloudera.framework.example.stream.Stream.PROPERTY_POLL_TICKS;
import static com.cloudera.framework.example.stream.Stream.PROPERTY_RECORD_TYPE;
import static com.cloudera.framework.testing.Assert.assertCounterEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Map;

import com.cloudera.framework.common.Driver;
import com.cloudera.framework.example.TestBase;
import com.cloudera.framework.example.model.RecordCounter;
import com.cloudera.framework.example.model.RecordFactory;
import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.FlumeServer;
import com.google.common.collect.ImmutableMap;
import com.googlecode.zohhak.api.Coercion;
import com.googlecode.zohhak.api.TestWith;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.sink.hdfs.HDFSEventSink;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

/**
 * Test dataset stream
 */
@RunWith(TestRunner.class)
@SuppressWarnings("unchecked")
public class Stream extends TestBase {

  private static final String FLUME_CONFIG_FILE = "flume/flume-conf.properties";
  private static final String FLUME_AGENT_NAME = "mydataset";
  private static final String FLUME_SOURCE_POLL_MS = "25";
  private static final String KEY_FLUME_SOURCE_NAME = "sourceName";
  private static final String KEY_FLUME_SINK_NAME = "sinkName";
  private static final String KEY_FLUME_OUTPUT_DIR = "outputDir";
  private static final String KEY_FLUME_PROCESS_ITERATIONS = "iterations";
  private static final String KEY_FLUME_PROCESS_FILE_COUNT = "fileCount";

  @ClassRule
  public static DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static FlumeServer flumeServer = FlumeServer.getInstance();

  public final TestMetaData testMetaDataCsvBatch = TestMetaData.getInstance() //
    .parameters( //
      ImmutableMap.of( //
        PROPERTY_POLL_MS, FLUME_SOURCE_POLL_MS, //
        PROPERTY_POLL_TICKS, "1", //
        PROPERTY_BATCH_SIZE, "5", //
        PROPERTY_RECORD_TYPE, RecordFactory.RECORD_STRING_SERDE_CSV //
      ), //
      ImmutableMap.of(//
        com.cloudera.framework.example.stream.Stream.PROPERTY_BATCH_SIZE, "5" //
      ), //
      ImmutableMap.of(//
        KEY_FLUME_SOURCE_NAME, "source_single", //
        KEY_FLUME_SINK_NAME, "sink_batch_hdfs", //
        KEY_FLUME_OUTPUT_DIR, DIR_ABS_MYDS_STAGED, //
        KEY_FLUME_PROCESS_ITERATIONS, 3, //
        KEY_FLUME_PROCESS_FILE_COUNT, 1)) //
    .asserts( //
      ImmutableMap.of( //
        com.cloudera.framework.example.process.Stage.class.getCanonicalName(),
        ImmutableMap.of( //
          RecordCounter.FILES, 0L, //
          RecordCounter.FILES_CANONICAL, 0L, //
          RecordCounter.FILES_DUPLICATE, 0L, //
          RecordCounter.FILES_MALFORMED, 0L //
        )), //
      ImmutableMap.of( //
        com.cloudera.framework.example.process.Partition.class.getCanonicalName(),
        ImmutableMap.of( //
          RecordCounter.RECORDS, 10000L, //
          RecordCounter.RECORDS_CANONICAL, 6000L, //
          RecordCounter.RECORDS_DUPLICATE, 4000L, //
          RecordCounter.RECORDS_MALFORMED, 0L //
        )), //
      ImmutableMap.of( //
        com.cloudera.framework.example.process.Cleanse.class.getCanonicalName(),
        ImmutableMap.of( //
          RecordCounter.RECORDS, 6000L, //
          RecordCounter.RECORDS_CANONICAL, 6000L, //
          RecordCounter.RECORDS_DUPLICATE, 0L, //
          RecordCounter.RECORDS_MALFORMED, 0L //
        )));
  public final TestMetaData testMetaDataXmlBatch = TestMetaData.getInstance() //
    .parameters( //
      ImmutableMap.of( //
        PROPERTY_POLL_MS, FLUME_SOURCE_POLL_MS, //
        PROPERTY_POLL_TICKS, "1", //
        PROPERTY_BATCH_SIZE, "5", //
        PROPERTY_RECORD_TYPE, RecordFactory.RECORD_STRING_SERDE_XML //
      ), //
      ImmutableMap.of(//
        com.cloudera.framework.example.stream.Stream.PROPERTY_BATCH_SIZE, "5" //
      ), //
      ImmutableMap.of(//
        KEY_FLUME_SOURCE_NAME, "source_single", //
        KEY_FLUME_SINK_NAME, "sink_batch_hdfs", //
        KEY_FLUME_OUTPUT_DIR, DIR_ABS_MYDS_STAGED, //
        KEY_FLUME_PROCESS_ITERATIONS, 3, //
        KEY_FLUME_PROCESS_FILE_COUNT, 1)) //
    .asserts( //
      ImmutableMap.of( //
        com.cloudera.framework.example.process.Stage.class.getCanonicalName(),
        ImmutableMap.of( //
          RecordCounter.FILES, 0L, //
          RecordCounter.FILES_CANONICAL, 0L, //
          RecordCounter.FILES_DUPLICATE, 0L, //
          RecordCounter.FILES_MALFORMED, 0L //
        )), //
      ImmutableMap.of( //
        com.cloudera.framework.example.process.Partition.class.getCanonicalName(),
        ImmutableMap.of( //
          RecordCounter.RECORDS, 10000L, //
          RecordCounter.RECORDS_CANONICAL, 6000L, //
          RecordCounter.RECORDS_DUPLICATE, 4000L, //
          RecordCounter.RECORDS_MALFORMED, 0L //
        )), //
      ImmutableMap.of( //
        com.cloudera.framework.example.process.Cleanse.class.getCanonicalName(),
        ImmutableMap.of( //
          RecordCounter.RECORDS, 6000L, //
          RecordCounter.RECORDS_CANONICAL, 6000L, //
          RecordCounter.RECORDS_DUPLICATE, 0L, //
          RecordCounter.RECORDS_MALFORMED, 0L //
        )));
  private final Map<String, String> FLUME_SUBSTITUTIONS = ImmutableMap.of(//
    "FLUME_AGENT_NAME", FLUME_AGENT_NAME, //
    "ROOT_HDFS", dfsServer.getPathUri("/"), //
    "ROOT_DIR_HDFS", DIR_ABS_MYDS, //
    "RECORD_FORMAT", "xml"//
  );

  /**
   * Test dataset stream
   */
  @TestWith({"testMetaDataCsvBatch", "testMetaDataXmlBatch"})
  public void testStream(TestMetaData testMetaData) throws IOException, EventDeliveryException, InterruptedException {
    assertEquals(((Integer) testMetaData.getParameters()[2].get(KEY_FLUME_PROCESS_FILE_COUNT)).intValue(),
      flumeServer.crankPipeline(FLUME_SUBSTITUTIONS, FLUME_CONFIG_FILE, testMetaData.getParameters()[0], testMetaData.getParameters()[1],
        FLUME_AGENT_NAME, (String) testMetaData.getParameters()[2].get(KEY_FLUME_SOURCE_NAME),
        (String) testMetaData.getParameters()[2].get(KEY_FLUME_SINK_NAME), new com.cloudera.framework.example.stream.Stream(),
        new HDFSEventSink(), (String) testMetaData.getParameters()[2].get(KEY_FLUME_OUTPUT_DIR),
        (Integer) testMetaData.getParameters()[2].get(KEY_FLUME_PROCESS_ITERATIONS)));
    Driver driverStage = new com.cloudera.framework.example.process.Stage(dfsServer.getConf());
    assertEquals(Driver.RETURN_SUCCESS, driverStage.runner(
      new String[]{dfsServer.getPath(DIR_ABS_MYDS_RAW_CANONICAL).toString(), dfsServer.getPath(DIR_ABS_MYDS_STAGED).toString()}));
    assertCounterEquals(testMetaData.getAsserts()[0], driverStage.getCounters());
    Driver driverPartition = new com.cloudera.framework.example.process.Partition(dfsServer.getConf());
    assertEquals(Driver.RETURN_SUCCESS, driverPartition.runner(new String[]{dfsServer.getPath(DIR_ABS_MYDS_STAGED_CANONICAL).toString(),
      dfsServer.getPath(DIR_ABS_MYDS_PARTITIONED).toString()}));
    assertCounterEquals(testMetaData.getAsserts()[1], driverPartition.getCounters());
    Driver driverProcess = new com.cloudera.framework.example.process.Cleanse(dfsServer.getConf());
    assertEquals(Driver.RETURN_SUCCESS, driverProcess.runner(new String[]{
      dfsServer.getPath(DIR_ABS_MYDS_PARTITIONED_CANONICAL).toString(), dfsServer.getPath(DIR_ABS_MYDS_CLEANSED).toString()}));
    assertCounterEquals(testMetaData.getAsserts()[2], driverProcess.getCounters());
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
