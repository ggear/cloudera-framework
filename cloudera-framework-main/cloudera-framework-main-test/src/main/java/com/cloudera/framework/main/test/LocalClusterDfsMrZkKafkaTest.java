package com.cloudera.framework.main.test;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRConfig;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.framework.main.test.cluster.MiniKafka;
import com.cloudera.framework.main.test.cluster.MiniZooKeeper;

/**
 * Base class for all local-cluster DFS, MR, ZK and Kafka tests, single-process,
 * multi-threaded DFS facade over local file system and local MR job runner,
 * provides fast, functional read/write API compatibility, isolated and
 * idempotent runtime
 */
public class LocalClusterDfsMrZkKafkaTest extends BaseTest {

  private static Logger LOG = LoggerFactory.getLogger(LocalClusterDfsMrZkKafkaTest.class);

  private static JobConf conf;
  private static FileSystem fileSystem;
  private static MiniZooKeeper miniZooKeeper;
  private static MiniKafka miniKafka;

  public LocalClusterDfsMrZkKafkaTest() {
    super();
  }

  public LocalClusterDfsMrZkKafkaTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets,
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

  /**
   * Get the ZooKeeper instance
   */
  public MiniZooKeeper getZooKeeper() {
    return miniZooKeeper;
  }

  /**
   * Get the Kafka instance
   */
  public MiniKafka getKafka() {
    return miniKafka;
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
    miniKafka = new MiniKafka(miniZooKeeper = new MiniZooKeeper());
    miniZooKeeper.start();
    miniKafka.start();
    debugMessageFooter(LOG, "setUpRuntime", time);
  }

  @Before
  public void setUpZookeeper() throws Exception {
    long time = debugMessageHeader(LOG, "setUpZookeeper");
    if (LOG.isWarnEnabled()) {
      LOG.warn(LOG_PREFIX + " [setUpZookeeper] does not flush state between tests");
    }
    debugMessageFooter(LOG, "setUpZookeeper", time);
  }

  @Before
  public void setUpKafka() throws Exception {
    long time = debugMessageHeader(LOG, "setUpKafka");
    if (LOG.isWarnEnabled()) {
      LOG.warn(LOG_PREFIX + " [setUpKafka] does not flush state between tests");
    }
    debugMessageFooter(LOG, "setUpKafka", time);
  }

  @Before
  public void setUpFlume() throws Exception {
    long time = debugMessageHeader(LOG, "setUpFlume");
    debugMessageFooter(LOG, "setUpFlume", time);
  }

  @AfterClass
  public static void tearDownRuntime() throws Exception {
    long time = debugMessageHeader(LOG, "tearDownRuntime");
    miniKafka.stop();
    miniZooKeeper.stop();
    if (fileSystem != null) {
      fileSystem.close();
    }
    debugMessageFooter(LOG, "tearDownRuntime", time);
  }

}
