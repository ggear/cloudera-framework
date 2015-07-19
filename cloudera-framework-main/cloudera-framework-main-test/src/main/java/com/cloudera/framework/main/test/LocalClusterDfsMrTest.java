package com.cloudera.framework.main.test;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for all local-cluster DFS and MR tests, single process,
 * multi-threaded DFS facade over local file system and local MR job runner,
 * provides fast, functional read/write API compatibility, provides isolated and
 * idempotent runtime
 */
public class LocalClusterDfsMrTest extends BaseTest {

  private static Logger LOG = LoggerFactory.getLogger(LocalClusterDfsMrTest.class);

  private static JobConf conf;
  private static FileSystem fileSystem;

  public LocalClusterDfsMrTest() {
    super();
  }

  public LocalClusterDfsMrTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets,
      String[][][] labels, @SuppressWarnings("rawtypes") Map[] counters) {
    super(sources, destinations, datasets, subsets, labels, counters);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public FileSystem getFileSystem() {
    return fileSystem;
  }

  @Override
  public String getPathDfs(String path) {
    String pathRelativeToDfsRootSansLeadingSlashes = stripLeadingSlashes(path);
    return pathRelativeToDfsRootSansLeadingSlashes.equals("") ? REL_DIR_DFS_LOCAL
        : new Path(REL_DIR_DFS_LOCAL, pathRelativeToDfsRootSansLeadingSlashes).toUri().toString();
  }

  @BeforeClass
  public static void setUpRuntime() throws Exception {
    long time = debugMessageHeader(LOG, "setUpRuntime");
    fileSystem = FileSystem.getLocal(conf = new JobConf());
    conf.set(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
    debugMessageFooter(LOG, "setUpRuntime", time);
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
