package com.cloudera.example.stream;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.flume.EventDeliveryException;
import org.apache.flume.sink.hdfs.HDFSEventSink;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cloudera.example.TestConstants;
import com.cloudera.example.model.RecordCounter;
import com.cloudera.example.model.RecordFactory;
import com.cloudera.example.partition.Partition;
import com.cloudera.example.process.Process;
import com.cloudera.example.stage.Stage;
import com.cloudera.framework.main.common.Driver;
import com.cloudera.framework.main.test.LocalClusterDfsMrFlumeTest;
import com.google.common.collect.ImmutableMap;

/**
 * Test dataset stream
 */
@RunWith(Parameterized.class)
public class StreamTest extends LocalClusterDfsMrFlumeTest implements TestConstants {

  /**
   * Paramaterise the unit tests
   */
  @Parameters
  public static Iterable<Object[]> parameters() {
    return Arrays.asList(new Object[][] {
        // Single flume pipeline, CSV
        {
            // No datasets
            null, null, null, null, null,
            // Flume pipeline
            new Map[] {
                // Source overlay properties
                ImmutableMap.of(//
                    Stream.PROPERTY_POLL_MS, FLUME_SOURCE_POLL_MS, //
                    Stream.PROPERTY_POLL_TICKS, "0", //
                    Stream.PROPERTY_BATCH_SIZE, "1", //
                    Stream.PROPERTY_RECORD_TYPE, RecordFactory.RECORD_STRING_SERDE_CSV//
            ), //
               // Sink overlay properties
                ImmutableMap.of(//
                    Stream.PROPERTY_BATCH_SIZE, "1"//
            ), //
               // Pipeline properties
                ImmutableMap.of(//
                    KEY_FLUME_SOURCE_NAME, "source_single", //
                    KEY_FLUME_SINK_NAME, "sink_single_hdfs", //
                    KEY_FLUME_OUTPUT_DIR, DIR_ABS_MYDS_RAW, //
                    KEY_FLUME_PROCESS_ITERATIONS, 3, //
                    KEY_FLUME_PROCESS_FILE_COUNT, 3//
            ), //
               // Stage counters
                ImmutableMap.of(Stage.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.FILES, 3L, //
                        RecordCounter.FILES_CANONICAL, 3L, //
                        RecordCounter.FILES_DUPLICATE, 0L, //
                        RecordCounter.FILES_MALFORMED, 0L //
            )), //
                // Partition counters
                ImmutableMap.of(Partition.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 3000L, //
                        RecordCounter.RECORDS_CANONICAL, 3000L, //
                        RecordCounter.RECORDS_DUPLICATE, 0L, //
                        RecordCounter.RECORDS_MALFORMED, 0L//
            )), //
                // Process counters
                ImmutableMap.of(Process.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 3000L, //
                        RecordCounter.RECORDS_CANONICAL, 3000L, //
                        RecordCounter.RECORDS_DUPLICATE, 0L, //
                        RecordCounter.RECORDS_MALFORMED, 0L//
            )), //
        }, //
        }, //
        // Batch flume pipeline, CSV
        {
            // No datasets
            null, null, null, null, null,
            // Flume pipeline
            new Map[] {
                // Source overlay properties
                ImmutableMap.of(//
                    Stream.PROPERTY_POLL_MS, FLUME_SOURCE_POLL_MS, //
                    Stream.PROPERTY_POLL_TICKS, "1", //
                    Stream.PROPERTY_BATCH_SIZE, "5", //
                    Stream.PROPERTY_RECORD_TYPE, RecordFactory.RECORD_STRING_SERDE_CSV//
            ), //
               // Sink overlay properties
                ImmutableMap.of(//
                    Stream.PROPERTY_BATCH_SIZE, "5"//
            ), //
               // Pipeline properties
                ImmutableMap.of(//
                    KEY_FLUME_SOURCE_NAME, "source_single", //
                    KEY_FLUME_SINK_NAME, "sink_batch_hdfs", //
                    KEY_FLUME_OUTPUT_DIR, DIR_ABS_MYDS_STAGED, //
                    KEY_FLUME_PROCESS_ITERATIONS, 3, //
                    KEY_FLUME_PROCESS_FILE_COUNT, 1//
            ), //
               // Stage counters
                ImmutableMap.of(Stage.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.FILES, 0L, //
                        RecordCounter.FILES_CANONICAL, 0L, //
                        RecordCounter.FILES_DUPLICATE, 0L, //
                        RecordCounter.FILES_MALFORMED, 0L //
            )), //
                // Partition counters
                ImmutableMap.of(Partition.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 5000L, //
                        RecordCounter.RECORDS_CANONICAL, 3000L, //
                        RecordCounter.RECORDS_DUPLICATE, 2000L, //
                        RecordCounter.RECORDS_MALFORMED, 0L//
            )), //
                // Process counters
                ImmutableMap.of(Process.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 3000L, //
                        RecordCounter.RECORDS_CANONICAL, 3000L, //
                        RecordCounter.RECORDS_DUPLICATE, 0L, //
                        RecordCounter.RECORDS_MALFORMED, 0L//
            )), //
        }, //
        }, //
        // Single flume pipeline, XML
        {
            // No datasets
            null, null, null, null, null,
            // Flume pipeline
            new Map[] {
                // Source overlay properties
                ImmutableMap.of(//
                    Stream.PROPERTY_POLL_MS, FLUME_SOURCE_POLL_MS, //
                    Stream.PROPERTY_POLL_TICKS, "0", //
                    Stream.PROPERTY_BATCH_SIZE, "1", //
                    Stream.PROPERTY_RECORD_TYPE, RecordFactory.RECORD_STRING_SERDE_XML//
            ), //
               // Sink overlay properties
                ImmutableMap.of(//
                    Stream.PROPERTY_BATCH_SIZE, "1"//
            ), //
               // Pipeline properties
                ImmutableMap.of(//
                    KEY_FLUME_SOURCE_NAME, "source_single", //
                    KEY_FLUME_SINK_NAME, "sink_single_hdfs", //
                    KEY_FLUME_OUTPUT_DIR, DIR_ABS_MYDS_RAW, //
                    KEY_FLUME_PROCESS_ITERATIONS, 3, //
                    KEY_FLUME_PROCESS_FILE_COUNT, 3//
            ), //
               // Stage counters
                ImmutableMap.of(Stage.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.FILES, 3L, //
                        RecordCounter.FILES_CANONICAL, 3L, //
                        RecordCounter.FILES_DUPLICATE, 0L, //
                        RecordCounter.FILES_MALFORMED, 0L //
            )), //
                // Partition counters
                ImmutableMap.of(Partition.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 3000L, //
                        RecordCounter.RECORDS_CANONICAL, 3000L, //
                        RecordCounter.RECORDS_DUPLICATE, 0L, //
                        RecordCounter.RECORDS_MALFORMED, 0L//
            )), //
                // Process counters
                ImmutableMap.of(Process.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 3000L, //
                        RecordCounter.RECORDS_CANONICAL, 3000L, //
                        RecordCounter.RECORDS_DUPLICATE, 0L, //
                        RecordCounter.RECORDS_MALFORMED, 0L//
            )), //
        }, //
        }, //
        // Batch flume pipeline, XML
        {
            // No datasets
            null, null, null, null, null,
            // Flume pipeline
            new Map[] {
                // Source overlay properties
                ImmutableMap.of(//
                    Stream.PROPERTY_POLL_MS, FLUME_SOURCE_POLL_MS, //
                    Stream.PROPERTY_POLL_TICKS, "1", //
                    Stream.PROPERTY_BATCH_SIZE, "5", //
                    Stream.PROPERTY_RECORD_TYPE, RecordFactory.RECORD_STRING_SERDE_XML//
            ), //
               // Sink overlay properties
                ImmutableMap.of(//
                    Stream.PROPERTY_BATCH_SIZE, "5"//
            ), //
               // Pipeline properties
                ImmutableMap.of(//
                    KEY_FLUME_SOURCE_NAME, "source_single", //
                    KEY_FLUME_SINK_NAME, "sink_batch_hdfs", //
                    KEY_FLUME_OUTPUT_DIR, DIR_ABS_MYDS_STAGED, //
                    KEY_FLUME_PROCESS_ITERATIONS, 3, //
                    KEY_FLUME_PROCESS_FILE_COUNT, 1//
            ), //
               // Stage counters
                ImmutableMap.of(Stage.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.FILES, 0L, //
                        RecordCounter.FILES_CANONICAL, 0L, //
                        RecordCounter.FILES_DUPLICATE, 0L, //
                        RecordCounter.FILES_MALFORMED, 0L //
            )), //
                // Partition counters
                ImmutableMap.of(Partition.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 5000L, //
                        RecordCounter.RECORDS_CANONICAL, 3000L, //
                        RecordCounter.RECORDS_DUPLICATE, 2000L, //
                        RecordCounter.RECORDS_MALFORMED, 0L//
            )), //
                // Process counters
                ImmutableMap.of(Process.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 3000L, //
                        RecordCounter.RECORDS_CANONICAL, 3000L, //
                        RecordCounter.RECORDS_DUPLICATE, 0L, //
                        RecordCounter.RECORDS_MALFORMED, 0L//
            )), //
        }, //
        }, //
    });
  }

  /**
   * Test dataset stream
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testStream() throws IOException, EventDeliveryException, InterruptedException {
    Assert.assertEquals(((Integer) metadata[2].get(KEY_FLUME_PROCESS_FILE_COUNT)).intValue(),
        processSouceSinkPipeline(FLUME_SUBSTITUTIONS, FLUME_CONFIG_FILE, metadata[0], metadata[1], FLUME_AGENT_NAME,
            (String) metadata[2].get(KEY_FLUME_SOURCE_NAME), (String) metadata[2].get(KEY_FLUME_SINK_NAME),
            new Stream(), new HDFSEventSink(), (String) metadata[2].get(KEY_FLUME_OUTPUT_DIR),
            (Integer) metadata[2].get(KEY_FLUME_PROCESS_ITERATIONS)));
    Driver driverStage = new Stage(getConf());
    Assert.assertEquals(Driver.RETURN_SUCCESS,
        driverStage.runner(new String[] { getPathDfs(DIR_ABS_MYDS_RAW_CANONICAL), getPathDfs(DIR_ABS_MYDS_STAGED) }));
    assertCounterEquals(metadata[3], driverStage.getCounters());
    Driver driverPartition = new Partition(getConf());
    Assert.assertEquals(Driver.RETURN_SUCCESS, driverPartition
        .runner(new String[] { getPathDfs(DIR_ABS_MYDS_STAGED_CANONICAL), getPathDfs(DIR_ABS_MYDS_PARTITIONED) }));
    assertCounterEquals(metadata[4], driverPartition.getCounters());
    Driver driverProcess = new Process(getConf());
    Assert.assertEquals(Driver.RETURN_SUCCESS, driverProcess
        .runner(new String[] { getPathDfs(DIR_ABS_MYDS_PARTITIONED_CANONICAL), getPathDfs(DIR_ABS_MYDS_PROCESSED) }));
    assertCounterEquals(metadata[5], driverProcess.getCounters());
  }

  public StreamTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets, String[][][] labels,
      @SuppressWarnings("rawtypes") Map[] metadata) {
    super(sources, destinations, datasets, subsets, labels, metadata);
  }

  @Override
  public void setUpDatasets() throws IllegalArgumentException, IOException {
    // no need to copy datasets, the Flume source generates them
  }

  private static final Map<String, String> FLUME_SUBSTITUTIONS = ImmutableMap.of(//
      "ROOT_HDFS", new LocalClusterDfsMrFlumeTest().getPathDfs("/"), //
      "ROOT_DIR_HDFS_RAW", DIR_ABS_MYDS_RAW, //
      "ROOT_DIR_HDFS_STAGED", DIR_ABS_MYDS_STAGED, //
      "RECORD_FORMAT", "xml"//
  );
  private static final String FLUME_CONFIG_FILE = "cfg/flume/flume-conf.properties";
  private static final String FLUME_AGENT_NAME = "mydataset";
  private static final String FLUME_SOURCE_POLL_MS = "25";

  private static final String KEY_FLUME_SOURCE_NAME = "sourceName";
  private static final String KEY_FLUME_SINK_NAME = "sinkName";
  private static final String KEY_FLUME_OUTPUT_DIR = "outputDir";
  private static final String KEY_FLUME_PROCESS_ITERATIONS = "iterations";
  private static final String KEY_FLUME_PROCESS_FILE_COUNT = "fileCount";

}
