package com.cloudera.framework.main.test;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Serializable;

/**
 * Base class for all local-cluster DFS and Spark tests, single-process,
 * multi-threaded DFS facade over local file system and local mode Spark,
 * providing fast, functional read/write API compatibility, isolated and
 * idempotent runtime
 */
@SuppressWarnings("serial")
public class LocalClusterDfsSparkTest extends BaseTest implements Serializable {

  private static Logger LOG = LoggerFactory.getLogger(LocalClusterDfsSparkTest.class);

  private static JobConf conf;
  private static FileSystem fileSystem;
  private static JavaSparkContext context;

  public LocalClusterDfsSparkTest() {
    super();
  }

  public LocalClusterDfsSparkTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets,
      String[][][] labels, @SuppressWarnings("rawtypes") Map[] metadata) {
    super(sources, destinations, datasets, subsets, labels, metadata);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public FileSystem getFileSystem() {
    return fileSystem;
  }

  public JavaSparkContext getContext() {
    return context;
  }

  @Override
  public String getPathString(String path) {
    String pathRelativeToDfsRootSansLeadingSlashes = stripLeadingSlashes(path);
    return pathRelativeToDfsRootSansLeadingSlashes.equals("") ? REL_DIR_DFS_LOCAL
        : new Path(REL_DIR_DFS_LOCAL, pathRelativeToDfsRootSansLeadingSlashes).toUri().toString();
  }

  @Override
  public String getPathUri(String path) {
    return new Path(ABS_DIR_DFS_LOCAL, (path = stripLeadingSlashes(path)).equals("") ? "." : path)
        .makeQualified(getFileSystem().getUri(), PATH_ROOT).toString();
  }

  @BeforeClass
  public static void setUpRuntime() throws Exception {
    long time = debugMessageHeader(LOG, "setUpRuntime");
    fileSystem = FileSystem.getLocal(conf = new JobConf());
    conf.set(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
    debugMessageFooter(LOG, "setUpRuntime", time);
  }

  @Before
  public void setUpSparkContext() {
    long time = debugMessageHeader(LOG, "setUpSparkContext");
    context = new JavaSparkContext("local", "unit-test", new SparkConf().setAppName("Spark Unit-Test"));
    debugMessageFooter(LOG, "setUpSparkContext", time);
  }

  @After
  public void tearDownSparkContext() {
    long time = debugMessageHeader(LOG, "tearDownSparkContext");
    if (context != null) {
      context.close();
    }
    debugMessageFooter(LOG, "tearDownSparkContext", time);
  }

  @AfterClass
  public static void tearDownRuntime() throws Exception {
    long time = debugMessageHeader(LOG, "tearDownRuntime");
    if (fileSystem != null) {
      fileSystem.close();
    }
    debugMessageFooter(LOG, "tearDownRuntime", time);
  }

}
