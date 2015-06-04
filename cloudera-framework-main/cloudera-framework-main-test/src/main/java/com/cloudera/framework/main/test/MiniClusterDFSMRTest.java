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
  private static FileSystem fileSystem;

  @Override
  public Configuration getConf() throws Exception {
    return conf;
  }

  @Override
  public FileSystem getFileSystem() throws IOException {
    return fileSystem;
  }

  @Override
  public String getPathLocal(String pathRelativeToModuleRoot) throws Exception {
    throw new UnsupportedOperationException(
        "Local paths are not accessible in mini-cluster mode");
  }

  @Override
  public String getPathHDFS(String pathRelativeToHDFSRoot) throws Exception {
    return pathRelativeToHDFSRoot;
  }

  @BeforeClass
  public static void setUpRuntime() throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Test harness [setUpRuntime] starting");
    }
    conf = new JobConf();
    fileSystem = (miniDfs = ShimLoader.getHadoopShims().getMiniDfs(conf, 1,
        true, null)).getFileSystem();
    miniMr = ShimLoader.getHadoopShims().getMiniMrCluster(conf, 1,
        fileSystem.getUri().toString(), 1);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Test harness [setUpRuntime] finished");
    }
  }

  @AfterClass
  public static void tearDownRuntime() throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Test harness [tearDownRuntime] starting");
    }
    if (miniMr != null) {
      miniMr.shutdown();
    }
    if (miniDfs != null) {
      miniDfs.shutdown();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Test harness [tearDownRuntime] finished");
    }
  }

}
