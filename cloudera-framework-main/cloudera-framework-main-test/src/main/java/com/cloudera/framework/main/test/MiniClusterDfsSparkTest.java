package com.cloudera.framework.main.test;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.shims.HadoopShims.MiniDFSShim;
import org.apache.hadoop.hive.shims.HadoopShims.MiniMrShim;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.JobConf;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for all local-cluster DFS and Spark tests, single-process,
 * multi-threaded DFS and Spark daemons, exercises the full read/write path of
 * the stack providing isolated and idempotent runtime
 */
public class MiniClusterDfsSparkTest extends BaseTest {

  private static Logger LOG = LoggerFactory.getLogger(MiniClusterDfsSparkTest.class);

  private static Configuration conf;
  private static MiniDFSShim miniDfs;
  private static MiniMrShim miniSpark;
  private static FileSystem fileSystem;

  // TODO: Provide spark impl

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

  @BeforeClass
  public static void setUpRuntime() throws Exception {
    long time = debugMessageHeader(LOG, "setUpRuntime");
    JobConf jobConf = new JobConf();
    fileSystem = (miniDfs = ShimLoader.getHadoopShims().getMiniDfs(jobConf, 1, true, null)).getFileSystem();
    miniSpark = ShimLoader.getHadoopShims().getMiniSparkCluster(jobConf, 1, fileSystem.getUri().toString(), 1);
    conf = fileSystem.getConf();
    debugMessageFooter(LOG, "setUpRuntime", time);
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
