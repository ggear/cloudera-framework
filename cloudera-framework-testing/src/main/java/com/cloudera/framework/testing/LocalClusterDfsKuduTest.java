package com.cloudera.framework.testing;

import java.io.File;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRConfig;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduClient.KuduClientBuilder;
import org.kududb.client.MiniKuduCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for all local-cluster DFS and Kudu tests, single-process,
 * multi-threaded DFS facade over local file system and multi-process Kudu
 * cluster, providing fast, functional read/write API compatibility, isolated
 * and idempotent runtime
 */
public class LocalClusterDfsKuduTest extends BaseTest {

  private static Logger LOG = LoggerFactory.getLogger(LocalClusterDfsKuduTest.class);

  private static JobConf conf;
  private static FileSystem fileSystem;
  private static MiniKuduCluster miniKudu;

  private static int KUDU_MSERVER_NUM = 1;
  private static int KUDU_TSERVER_NUM = 3;

  private static String SYSTEM_PROPERTY_KUDU_HOME_BIN = "binDir";

  public LocalClusterDfsKuduTest() {
    super();
  }

  public LocalClusterDfsKuduTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets,
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
   * Get the Kudu instance
   */
  public MiniKuduCluster getKudu() {
    return miniKudu;
  }

  @Override
  public String getPathString(String path) {
    String pathRelativeToDfsRootSansLeadingSlashes = stripLeadingSlashes(path);
    return pathRelativeToDfsRootSansLeadingSlashes.equals("") ? REL_DIR_DFS_LOCAL
        : new Path(REL_DIR_DFS_LOCAL, pathRelativeToDfsRootSansLeadingSlashes).toUri().toString();
  }

  @BeforeClass
  public static void setUpRuntime() throws Exception {
    long time = debugMessageHeader(LOG, "setUpRuntime");
    fileSystem = FileSystem.getLocal(conf = new JobConf());
    conf.set(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
    File kuduHome = new File(ABS_DIR_KUDU_MINICLUSTER, "sbin");
    if (!kuduHome.exists()) {
      kuduHome = new File(ABS_DIR_KUDU_MINICLUSTER, "bin");
    }
    System.setProperty(SYSTEM_PROPERTY_KUDU_HOME_BIN, kuduHome.getAbsolutePath());
    File dataDir = new File(REL_DIR_KUDU_LOCAL);
    FileUtils.deleteDirectory(dataDir);
    dataDir.mkdirs();
    (miniKudu = new MiniKuduCluster.MiniKuduClusterBuilder().numMasters(KUDU_MSERVER_NUM).numTservers(KUDU_TSERVER_NUM)
        .defaultTimeoutMs(50000).build()).waitForTabletServers(KUDU_TSERVER_NUM);
    debugMessageFooter(LOG, "setUpRuntime", time);
  }

  @Before
  public void setUpKudu() throws Exception {
    long time = debugMessageHeader(LOG, "setUpKudu");
    KuduClient client = new KuduClientBuilder(getKudu().getMasterAddresses()).build();
    try {
      for (String table : client.getTablesList().getTablesList()) {
        client.deleteTable(table);
      }
    } finally {
      client.shutdown();
    }
    debugMessageFooter(LOG, "setUpKudu", time);
  }

  @AfterClass
  public static void tearDownRuntime() throws Exception {
    long time = debugMessageHeader(LOG, "tearDownRuntime");
    if (miniKudu != null) {
      miniKudu.shutdown();
    }
    if (fileSystem != null) {
      fileSystem.close();
    }
    debugMessageFooter(LOG, "tearDownRuntime", time);
  }

}
