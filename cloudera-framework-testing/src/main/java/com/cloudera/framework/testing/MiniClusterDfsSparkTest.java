package com.cloudera.framework.testing;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.shims.HadoopShims.MiniDFSShim;
import org.apache.hadoop.hive.shims.HadoopShims.MiniMrShim;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.JobConf;
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
 * multi-threaded DFS and Spark daemons, exercises the full read/write path of
 * the stack providing isolated and idempotent runtime
 */
@SuppressWarnings("serial")
public class MiniClusterDfsSparkTest extends BaseTest implements Serializable {

  private static Logger LOG = LoggerFactory.getLogger(MiniClusterDfsSparkTest.class);

  private static Configuration conf;
  private static MiniDFSShim miniDfs;
  private static MiniMrShim miniSpark;
  private static FileSystem fileSystem;
  private static JavaSparkContext context;

  public MiniClusterDfsSparkTest() {
    super();
  }

  public MiniClusterDfsSparkTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets,
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

  @BeforeClass
  public static void setUpRuntime() throws Exception {
    long time = debugMessageHeader(LOG, "setUpRuntime");
    JobConf jobConf = new JobConf();
    fileSystem = (miniDfs = ShimLoader.getHadoopShims().getMiniDfs(jobConf, 1, true, null)).getFileSystem();
    miniSpark = ShimLoader.getHadoopShims().getMiniSparkCluster(jobConf, 1, fileSystem.getUri().toString(), 1);
    conf = fileSystem.getConf();
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
    if (miniSpark != null) {
      miniSpark.shutdown();
    }
    if (miniDfs != null) {
      miniDfs.shutdown();
    }
    debugMessageFooter(LOG, "tearDownRuntime", time);
  }

}
