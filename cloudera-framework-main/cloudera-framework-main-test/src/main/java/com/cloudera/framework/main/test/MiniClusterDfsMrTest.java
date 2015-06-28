package com.cloudera.framework.main.test;

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

public class MiniClusterDfsMrTest extends BaseTest {

  private static Logger LOG = LoggerFactory.getLogger(MiniClusterDfsMrTest.class);

  private static Configuration conf;
  private static MiniDFSShim miniDfs;
  private static MiniMrShim miniMr;
  private static FileSystem fileSystem;

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
    miniMr = ShimLoader.getHadoopShims().getMiniMrCluster(jobConf, 1, fileSystem.getUri().toString(), 1);
    conf = fileSystem.getConf();
    debugMessageFooter(LOG, "setUpRuntime", time);
  }

  @AfterClass
  public static void tearDownRuntime() throws Exception {
    long time = debugMessageHeader(LOG, "tearDownRuntime");
    if (miniMr != null) {
      miniMr.shutdown();
    }
    if (miniDfs != null) {
      miniDfs.shutdown();
    }
    debugMessageFooter(LOG, "tearDownRuntime", time);
  }

}
