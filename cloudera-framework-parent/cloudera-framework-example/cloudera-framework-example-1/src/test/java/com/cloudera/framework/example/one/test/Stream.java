package com.cloudera.framework.example.one.test;

import static com.cloudera.framework.common.Driver.SUCCESS;
import static com.cloudera.framework.example.one.stream.Stream.PROPERTY_BATCH_SIZE;
import static com.cloudera.framework.example.one.stream.Stream.PROPERTY_POLL_MS;
import static com.cloudera.framework.example.one.stream.Stream.PROPERTY_POLL_TICKS;
import static com.cloudera.framework.example.one.stream.Stream.PROPERTY_RECORD_TYPE;
import static com.cloudera.framework.testing.Assert.assertCounterEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Map;

import com.cloudera.framework.common.Driver;
import com.cloudera.framework.example.one.TestBase;
import com.cloudera.framework.example.one.model.RecordCounter;
import com.cloudera.framework.example.one.model.RecordFactory;
import com.cloudera.framework.example.one.process.Cleanse;
import com.cloudera.framework.example.one.process.Partition;
import com.cloudera.framework.example.one.process.Stage;
import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.FlumeServer;
import com.google.common.collect.ImmutableMap;
import com.googlecode.zohhak.api.Coercion;
import com.googlecode.zohhak.api.TestWith;
import edu.umd.cs.findbugs.annotations.SuppressWarnings;
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

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static final FlumeServer flumeServer = FlumeServer.getInstance();

  private static final String FLUME_CONFIG_FILE = "flume/flume-conf.properties";
  private static final String FLUME_AGENT_NAME = "mydataset";
  private static final String FLUME_SOURCE_POLL_MS = "25";
  private static final String KEY_FLUME_SOURCE_NAME = "sourceName";
  private static final String KEY_FLUME_SINK_NAME = "sinkName";
  private static final String KEY_FLUME_OUTPUT_DIR = "outputDir";
  private static final String KEY_FLUME_PROCESS_ITERATIONS = "iterations";
  private static final String KEY_FLUME_PROCESS_FILE_COUNT = "fileCount";

  public final TestMetaData testMetaDataCsvBatch = TestMetaData.getInstance() //
    .parameters( //
      ImmutableMap.of( //
        PROPERTY_POLL_MS, FLUME_SOURCE_POLL_MS, //
        PROPERTY_POLL_TICKS, "1", //
        PROPERTY_BATCH_SIZE, "5", //
        PROPERTY_RECORD_TYPE, RecordFactory.RECORD_STRING_SERDE_CSV //
      ), //
      ImmutableMap.of(//
        com.cloudera.framework.example.one.stream.Stream.PROPERTY_BATCH_SIZE, "5" //
      ), //
      ImmutableMap.of(//
        KEY_FLUME_SOURCE_NAME, "source_single", //
        KEY_FLUME_SINK_NAME, "sink_batch_hdfs", //
        KEY_FLUME_OUTPUT_DIR, DIR_ABS_MYDS_STAGED, //
        KEY_FLUME_PROCESS_ITERATIONS, 3, //
        KEY_FLUME_PROCESS_FILE_COUNT, 2)) //
    .asserts( //
      ImmutableMap.of( //
        Stage.class.getName(),
        ImmutableMap.of( //
          RecordCounter.FILES, 0L, //
          RecordCounter.FILES_CANONICAL, 0L, //
          RecordCounter.FILES_DUPLICATE, 0L, //
          RecordCounter.FILES_MALFORMED, 0L //
        )), //
      ImmutableMap.of( //
        Partition.class.getName(),
        ImmutableMap.of( //
          RecordCounter.RECORDS, 20000L, //
          RecordCounter.RECORDS_CANONICAL, 10000L, //
          RecordCounter.RECORDS_DUPLICATE, 10000L, //
          RecordCounter.RECORDS_MALFORMED, 0L //
        )), //
      ImmutableMap.of( //
        com.cloudera.framework.example.one.process.Cleanse.class.getName(),
        ImmutableMap.of( //
          RecordCounter.RECORDS, 10000L, //
          RecordCounter.RECORDS_CANONICAL, 10000L, //
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
        com.cloudera.framework.example.one.stream.Stream.PROPERTY_BATCH_SIZE, "5" //
      ), //
      ImmutableMap.of(//
        KEY_FLUME_SOURCE_NAME, "source_single", //
        KEY_FLUME_SINK_NAME, "sink_batch_hdfs", //
        KEY_FLUME_OUTPUT_DIR, DIR_ABS_MYDS_STAGED, //
        KEY_FLUME_PROCESS_ITERATIONS, 3, //
        KEY_FLUME_PROCESS_FILE_COUNT, 2)) //
    .asserts( //
      ImmutableMap.of( //
        Stage.class.getName(),
        ImmutableMap.of( //
          RecordCounter.FILES, 0L, //
          RecordCounter.FILES_CANONICAL, 0L, //
          RecordCounter.FILES_DUPLICATE, 0L, //
          RecordCounter.FILES_MALFORMED, 0L //
        )), //
      ImmutableMap.of( //
        Partition.class.getName(),
        ImmutableMap.of( //
          RecordCounter.RECORDS, 20000L, //
          RecordCounter.RECORDS_CANONICAL, 10000L, //
          RecordCounter.RECORDS_DUPLICATE, 10000L, //
          RecordCounter.RECORDS_MALFORMED, 0L //
        )), //
      ImmutableMap.of( //
        Cleanse.class.getName(),
        ImmutableMap.of( //
          RecordCounter.RECORDS, 10000L, //
          RecordCounter.RECORDS_CANONICAL, 10000L, //
          RecordCounter.RECORDS_DUPLICATE, 0L, //
          RecordCounter.RECORDS_MALFORMED, 0L //
        )));

  private final Map<String, String> FLUME_SUBSTITUTIONS = ImmutableMap.of(//
    "FLUME_AGENT_NAME", FLUME_AGENT_NAME, //
    "HDFS_URL", dfsServer.getPathUri("/"), //
    "HDFS_APP", DIR_ABS_MYDS, //
    "RECORD_FORMAT", "xml"//
  );

  /**
   * Test dataset stream
   */
  @java.lang.SuppressWarnings("unchecked")
  @TestWith({"testMetaDataCsvBatch", "testMetaDataXmlBatch"})
  public void testStream(TestMetaData testMetaData) throws IOException, EventDeliveryException, InterruptedException {
    assertEquals(testMetaData.<Integer>getParameter(2, KEY_FLUME_PROCESS_FILE_COUNT).intValue(),
      flumeServer.crankPipeline(FLUME_SUBSTITUTIONS, FLUME_CONFIG_FILE, testMetaData.getParameters()[0], testMetaData.getParameters()[1],
        FLUME_AGENT_NAME, testMetaData.getParameter(2, KEY_FLUME_SOURCE_NAME),
        testMetaData.getParameter(2, KEY_FLUME_SINK_NAME), new com.cloudera.framework.example.one.stream.Stream(),
        new HDFSEventSink(), testMetaData.getParameter(2, KEY_FLUME_OUTPUT_DIR),
        testMetaData.<Integer>getParameter(2, KEY_FLUME_PROCESS_ITERATIONS), iterations -> 1));
    Driver driverStage = new Stage(dfsServer.getConf());
    assertEquals(SUCCESS, driverStage.runner(
      dfsServer.getPath(DIR_ABS_MYDS_RAW_CANONICAL).toString(), dfsServer.getPath(DIR_ABS_MYDS_STAGED).toString()));
    assertCounterEquals(testMetaData, driverStage.getCounters());
    Driver driverPartition = new Partition(dfsServer.getConf());
    assertEquals(SUCCESS, driverPartition.runner(dfsServer.getPath(DIR_ABS_MYDS_STAGED_CANONICAL).toString(),
      dfsServer.getPath(DIR_ABS_MYDS_PARTITIONED).toString()));
    assertCounterEquals(testMetaData, 1, driverPartition.getCounters());
    Driver driverProcess = new Cleanse(dfsServer.getConf());
    assertEquals(SUCCESS, driverProcess.runner(dfsServer.getPath(DIR_ABS_MYDS_PARTITIONED_CANONICAL).toString(), dfsServer.getPath
      (DIR_ABS_MYDS_CLEANSED).toString()));
    assertCounterEquals(testMetaData, 2, driverProcess.getCounters());
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
