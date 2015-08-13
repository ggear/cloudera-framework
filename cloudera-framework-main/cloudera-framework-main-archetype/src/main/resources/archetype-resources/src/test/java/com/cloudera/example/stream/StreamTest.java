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
                    KEY_FLUME_OUTPUT_DIR, DIR_DS_MYDATASET_RAW, //
                    KEY_FLUME_PROCESS_ITERATIONS, 3, //
                    KEY_FLUME_PROCESS_FILE_COUNT, 3//
            ), //
               // Stage counters
                ImmutableMap.of(Stage.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.FILES, 3L, //
                        RecordCounter.FILES_STAGED, 3L, //
                        RecordCounter.FILES_MALFORMED, 0L //
            )), //
                // Process counters
                ImmutableMap.of(Process.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 30L, //
                        RecordCounter.RECORDS_CLEANSED, 30L, //
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
                    Stream.PROPERTY_BATCH_SIZE, "2", //
                    Stream.PROPERTY_RECORD_TYPE, RecordFactory.RECORD_STRING_SERDE_CSV//
            ), //
               // Sink overlay properties
                ImmutableMap.of(//
                    Stream.PROPERTY_BATCH_SIZE, "2"//
            ), //
               // Pipeline properties
                ImmutableMap.of(//
                    KEY_FLUME_SOURCE_NAME, "source_single", //
                    KEY_FLUME_SINK_NAME, "sink_batch_hdfs", //
                    KEY_FLUME_OUTPUT_DIR, DIR_DS_MYDATASET_STAGED, //
                    KEY_FLUME_PROCESS_ITERATIONS, 3, //
                    KEY_FLUME_PROCESS_FILE_COUNT, 1//
            ), //
               // Stage counters
                ImmutableMap.of(Stage.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.FILES, 0L, //
                        RecordCounter.FILES_STAGED, 0L, //
                        RecordCounter.FILES_MALFORMED, 0L //
            )), //
                // Process counters
                ImmutableMap.of(Process.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 20L, //
                        RecordCounter.RECORDS_CLEANSED, 20L, //
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
                    KEY_FLUME_OUTPUT_DIR, DIR_DS_MYDATASET_RAW, //
                    KEY_FLUME_PROCESS_ITERATIONS, 3, //
                    KEY_FLUME_PROCESS_FILE_COUNT, 3//
            ), //
               // Stage counters
                ImmutableMap.of(Stage.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.FILES, 3L, //
                        RecordCounter.FILES_STAGED, 3L, //
                        RecordCounter.FILES_MALFORMED, 0L //
            )), //
                // Process counters
                ImmutableMap.of(Process.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 30L, //
                        RecordCounter.RECORDS_CLEANSED, 30L, //
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
                    Stream.PROPERTY_BATCH_SIZE, "2", //
                    Stream.PROPERTY_RECORD_TYPE, RecordFactory.RECORD_STRING_SERDE_XML//
            ), //
               // Sink overlay properties
                ImmutableMap.of(//
                    Stream.PROPERTY_BATCH_SIZE, "2"//
            ), //
               // Pipeline properties
                ImmutableMap.of(//
                    KEY_FLUME_SOURCE_NAME, "source_single", //
                    KEY_FLUME_SINK_NAME, "sink_batch_hdfs", //
                    KEY_FLUME_OUTPUT_DIR, DIR_DS_MYDATASET_STAGED, //
                    KEY_FLUME_PROCESS_ITERATIONS, 3, //
                    KEY_FLUME_PROCESS_FILE_COUNT, 1//
            ), //
               // Stage counters
                ImmutableMap.of(Stage.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.FILES, 0L, //
                        RecordCounter.FILES_STAGED, 0L, //
                        RecordCounter.FILES_MALFORMED, 0L //
            )), //
                // Process counters
                ImmutableMap.of(Process.class.getCanonicalName(),
                    ImmutableMap.of(//
                        RecordCounter.RECORDS, 20L, //
                        RecordCounter.RECORDS_CLEANSED, 20L, //
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
  public void testStream() throws IOException, EventDeliveryException {
    Assert.assertEquals(((Integer) metadata[2].get(KEY_FLUME_PROCESS_FILE_COUNT)).intValue(),
        processSouceSinkPipeline(FLUME_SUBSTITUTIONS, FLUME_CONFIG_FILE, metadata[0], metadata[1], FLUME_AGENT_NAME,
            (String) metadata[2].get(KEY_FLUME_SOURCE_NAME), (String) metadata[2].get(KEY_FLUME_SINK_NAME),
            new Stream(), new HDFSEventSink(), (String) metadata[2].get(KEY_FLUME_OUTPUT_DIR),
            (Integer) metadata[2].get(KEY_FLUME_PROCESS_ITERATIONS)));
    Driver driverStage = new Stage(getConf());
    Assert.assertEquals(Driver.RETURN_SUCCESS, driverStage
        .runner(new String[] { getPathDfs(DIR_DS_MYDATASET_RAW_SOURCE), getPathDfs(DIR_DS_MYDATASET_STAGED) }));
    assertCounterEquals(metadata[3], driverStage.getCounters());
    Driver driverProcess = new Process(getConf());
    Assert.assertEquals(Driver.RETURN_SUCCESS, driverProcess.runner(
        new String[] { getPathDfs(DIR_DS_MYDATASET_STAGED_PARTITIONED), getPathDfs(DIR_DS_MYDATASET_PROCESSED) }));
    assertCounterEquals(metadata[4], driverProcess.getCounters());
  }

  public StreamTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets, String[][][] labels,
      @SuppressWarnings("rawtypes") Map[] metadata) {
    super(sources, destinations, datasets, subsets, labels, metadata);
  }

  @Override
  public void setupDatasets() throws IllegalArgumentException, IOException {
    // no need to copy datasets, the Flume source generates them
  }

  private static final Map<String, String> FLUME_SUBSTITUTIONS = ImmutableMap.of(//
      "HDFS_ROOT", new LocalClusterDfsMrFlumeTest().getPathDfs("/"), //
      "ROOT_DIR_HDFS_RAW", DIR_DS_MYDATASET_RAW, "ROOT_DIR_HDFS_STAGED", DIR_DS_MYDATASET_STAGED);
  private static final String FLUME_CONFIG_FILE = "cfg/flume/flume-conf.properties";
  private static final String FLUME_AGENT_NAME = "mydataset";
  private static final String FLUME_SOURCE_POLL_MS = "25";

  private static final String KEY_FLUME_SOURCE_NAME = "sourceName";
  private static final String KEY_FLUME_SINK_NAME = "sinkName";
  private static final String KEY_FLUME_OUTPUT_DIR = "outputDir";
  private static final String KEY_FLUME_PROCESS_ITERATIONS = "iterations";
  private static final String KEY_FLUME_PROCESS_FILE_COUNT = "fileCount";

}
