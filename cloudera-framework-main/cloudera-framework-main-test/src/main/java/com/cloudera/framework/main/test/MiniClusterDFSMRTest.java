package com.cloudera.framework.main.test;

import java.io.IOException;

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

public abstract class MiniClusterDFSMRTest extends BaseTest {

  private static Logger LOG = LoggerFactory
      .getLogger(MiniClusterDFSMRTest.class);

  private static JobConf conf;
  private static MiniDFSShim miniDfs;
  private static MiniMrShim miniMr;

  @Override
  public Configuration getConf() throws Exception {
    return conf;
  }

  @Override
  public FileSystem getFileSystem() throws IOException {
    return miniDfs == null ? null : miniDfs.getFileSystem();
  }

  @BeforeClass
  public static void setUpRuntime() throws Exception {
    conf = new JobConf();
    miniDfs = ShimLoader.getHadoopShims().getMiniDfs(conf, 1, true, null);
    miniMr = ShimLoader.getHadoopShims().getMiniMrCluster(conf, 1,
        miniDfs.getFileSystem().getUri().toString(), 1);
  }

  @AfterClass
  public static void tearDownRuntime() throws Exception {
    if (miniMr != null) {
      miniMr.shutdown();
    }
    if (miniDfs != null) {
      miniDfs.shutdown();
    }
  }

}
