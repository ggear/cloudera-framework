package com.cloudera.framework.testing.server.test;

import java.util.HashMap;
import java.util.Map;

import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.EnvelopeServer;
import com.cloudera.framework.testing.server.SparkServer;
import com.cloudera.labs.envelope.run.Runner;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.googlecode.zohhak.api.Coercion;
import com.googlecode.zohhak.api.TestWith;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

@RunWith(TestRunner.class)
public class TestEnvelopeServer implements TestConstants {

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance(DfsServer.Runtime.CLUSTER_DFS);

  @ClassRule
  public static final SparkServer sparkServer = SparkServer.getInstance();

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
    System.setProperty("DFS_INPUT", dfsServer.getPathUri(DATASET_DIR_INPUT));
    System.setProperty("DFS_OUTPUT", dfsServer.getPathUri(DATASET_DIR_OUTPUT));
    Runner.run(ConfigUtils.applySubstitutions(ConfigUtils.configFromPath(ENVELOPE_CONF)));
    Assert.assertEquals(2, dfsServer.listFilesDfs(DATASET_DIR_OUTPUT).length);
    Assert.assertTrue(dfsServer.getFileSystem().exists(new Path(DATASET_DIR_OUTPUT, "_SUCCESS")));
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
