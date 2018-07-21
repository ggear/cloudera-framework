package com.cloudera.framework.testing.server.test;

import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.EnvelopeServer;
import com.cloudera.labs.envelope.run.Runner;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.googlecode.zohhak.api.Coercion;
import com.googlecode.zohhak.api.TestWith;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

@RunWith(TestRunner.class)
public class TestEnvelopeServer implements TestConstants {

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static final EnvelopeServer envelopeServer = EnvelopeServer.getInstance();

  private static final String DATASET_NAME = "some-dataset";
  private static final String DATASET_DIR = "/tmp/" + DATASET_NAME;
  private static final String DATASET_DIR_INPUT = DATASET_DIR + "/input";
  private static final String DATASET_DIR_OUTPUT = DATASET_DIR + "/output";

  private static final String ENVELOPE_CONF = ABS_DIR_CLASSES_TEST + "/envelope/filesystem.conf";

  public final TestMetaData testMetaDataPristine = TestMetaData.getInstance().dataSetSourceDirs(REL_DIR_DATASET)
    .dataSetNames(DATASET_NAME).dataSetDestinationDirs(DATASET_DIR_INPUT);

  @TestWith({"testMetaDataPristine"})
  public void testEnvelope(TestMetaData testMetaData) throws Exception {
    String inputUri = dfsServer.getPathUri(DATASET_DIR_INPUT);
    String outputUri = dfsServer.getPathUri(DATASET_DIR_OUTPUT);
    System.setProperty("DFS_INPUT", inputUri);
    System.setProperty("DFS_OUTPUT", outputUri);
    Runner.run(ConfigUtils.applySubstitutions(ConfigUtils.configFromPath(ENVELOPE_CONF)));
    Assert.assertEquals(2, dfsServer.listFilesDfs(outputUri).length);
    Assert.assertTrue(dfsServer.getFileSystem().exists(new Path(outputUri, "_SUCCESS")));
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
