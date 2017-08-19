package com.cloudera.framework.example.three.test;


import static com.cloudera.framework.common.Driver.FAILURE_ARGUMENTS;
import static com.cloudera.framework.common.Driver.SUCCESS;
import static com.cloudera.framework.example.three.Driver.ModelDir;
import static com.cloudera.framework.example.three.Driver.ModelFile;
import static com.cloudera.framework.example.three.Driver.TestDir;
import static com.cloudera.framework.example.three.Driver.TrainDir;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.StringReader;

import javax.xml.transform.stream.StreamSource;

import com.cloudera.framework.common.Driver;
import com.cloudera.framework.example.three.ModelPmml;
import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.DfsServer.Runtime;
import com.cloudera.framework.testing.server.ScalaServer;
import com.cloudera.framework.testing.server.SparkServer;
import com.googlecode.zohhak.api.Coercion;
import com.googlecode.zohhak.api.TestWith;
import org.dmg.pmml.PMML;
import org.jpmml.model.JAXBUtil;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Test model
 */
@RunWith(TestRunner.class)
public class Model implements TestConstants {

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance(Runtime.CLUSTER_DFS);

  @ClassRule
  public static final SparkServer sparkServer = SparkServer.getInstance();

  @ClassRule
  public static final ScalaServer scalaServer = ScalaServer.getInstance();

  public static final String SCRIPT_SCALA = "scala/com/cloudera/framework/example/three/Model.scala";

  private static final String DATASET = "roomsensors";

  private static final String DATASET_TRAIN = "train";
  private static final String DATASET_TEST = "test";
  private static final String DATASET_DIR = "/tmp/" + DATASET;

  public final TestMetaData testMetaDataAll = TestMetaData.getInstance() //
    .dataSetSourceDirs(REL_DIR_DATASET, REL_DIR_DATASET) //
    .dataSetNames(DATASET, DATASET) //
    .dataSetSubsets(new String[][]{{DATASET_TRAIN}, {DATASET_TEST}}) //
    .dataSetLabels(new String[][][]{{{null},}, {{null},}}) //
    .dataSetDestinationDirs(DATASET_DIR + "/" + TrainDir(), DATASET_DIR + "/" + TestDir());

  /**
   * Test failure when incorrect arguments are passed
   */
  @Test
  public void testFailure() {
    assertEquals(FAILURE_ARGUMENTS, new com.cloudera.framework.example.three.Driver().runner());
  }

  /**
   * Test model library with no data
   */
  @Test
  public void testModelLibraryNoData() throws Exception {
    dfsServer.getFileSystem().mkdirs(dfsServer.getPath(DATASET_DIR));
    Driver driver = new com.cloudera.framework.example.three.Driver(dfsServer.getConf());
    assertEquals(SUCCESS, driver.runner(dfsServer.getPath(DATASET_DIR).toString()));
    assertFalse(dfsServer.getFileSystem().exists(dfsServer.getPath(DATASET_DIR + "/" + ModelDir() + "/" + ModelFile())));
  }

  /**
   * Test model library with sample training and test data
   */
  @TestWith({"testMetaDataAll"})
  public void testModelLibrary(TestMetaData testMetaData) throws Exception {
    Driver driver = new com.cloudera.framework.example.three.Driver(dfsServer.getConf());
    assertEquals(SUCCESS, driver.runner(dfsServer.getPath(DATASET_DIR).toString()));
    assertTrue(dfsServer.getFileSystem().exists(dfsServer.getPath(DATASET_DIR + "/" + ModelDir() + "/" + ModelFile())));
    assertTrue(Double.parseDouble(((PMML) JAXBUtil.unmarshal(new StreamSource(new StringReader((String) driver.getResults().get(0))))).
      getHeader().getExtensions().get(0).getContent().get(0).toString()) > ModelPmml.MinimumAccuracy());
    assertTrue(Double.parseDouble(((PMML) JAXBUtil.unmarshal(new StreamSource(dfsServer.getFileSystem().open(
      dfsServer.getPath(DATASET_DIR + "/" + ModelDir() + "/" + ModelFile()))))).
      getHeader().getExtensions().get(0).getContent().get(0).toString()) > ModelPmml.MinimumAccuracy());
  }

  /**
   * Test model script with sample training and test data
   */
  @Test
  public void testModelScript() throws Exception {
    assertEquals(0, scalaServer.execute(new File(DIR_SOURCE_SCRIPT, SCRIPT_SCALA)));
    assertTrue(dfsServer.getFileSystem().exists(dfsServer.getPath(DATASET_DIR + "/" + ModelDir() + "/" + ModelFile())));
    assertTrue(Double.parseDouble(((PMML) JAXBUtil.unmarshal(new StreamSource(dfsServer.getFileSystem().open(
      dfsServer.getPath(DATASET_DIR + "/" + ModelDir() + "/" + ModelFile()))))).
      getHeader().getExtensions().get(0).getContent().get(0).toString()) > ModelPmml.MinimumAccuracy());
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
