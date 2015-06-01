package com.cloudera.framework.main.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class LocalClusterDFSMRTest extends BaseTest {

  private static Logger LOG = LoggerFactory
      .getLogger(LocalClusterDFSMRTest.class);

  private static JobConf conf;
  private static FileSystem localDfs;

  @Override
  public Configuration getConf() throws Exception {
    return conf;
  }

  @Override
  public FileSystem getFileSystem() throws IOException {
    return localDfs;
  }

  @BeforeClass
  public static void setUpRuntime() throws Exception {
    localDfs = FileSystem.getLocal(conf = new JobConf());
  }

  @AfterClass
  public static void tearDownRuntime() throws Exception {
    if (localDfs != null) {
      localDfs.close();
    }
  }

}
