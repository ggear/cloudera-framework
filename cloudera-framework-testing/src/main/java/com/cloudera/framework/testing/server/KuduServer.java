package com.cloudera.framework.testing.server;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduClient.KuduClientBuilder;
import org.apache.kudu.client.MiniKuduCluster;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kudu {@link TestRule}
 */
public class KuduServer extends CdhServer<KuduServer, KuduServer.Runtime> {

  private static final Logger LOG = LoggerFactory.getLogger(KuduServer.class);

  private static final int KUDU_MSERVER_NUM = 1;
  private static final int KUDU_TSERVER_NUM = 3;

  private static final String SYSTEM_PROPERTY_KUDU_HOME_BIN = "binDir";

  private static KuduServer instance;
  private MiniKuduCluster miniKudu;

  private KuduServer(Runtime runtime) {
    super(runtime);
  }

  /**
   * Get instance with default runtime
   *
   * @return
   */
  public static synchronized KuduServer getInstance() {
    return getInstance(instance == null ? Runtime.CLUSTER_DEAMONS : instance.getRuntime());
  }

  /**
   * Get instance with specific <code>runtime</code>
   *
   * @return
   */
  public static synchronized KuduServer getInstance(Runtime runtime) {
    return instance == null ? instance = new KuduServer(runtime) : instance.assertRuntime(runtime);
  }

  /**
   * Get master addresses
   *
   * @return
   */
  public synchronized String getMasterAddresses() {
    return miniKudu.getMasterAddresses();
  }

  @Override
  public int getIndex() {
    return 20;
  }

  @Override
  public synchronized void start() throws Exception {
    long time = log(LOG, "start");
    File kuduBin = new File(ABS_DIR_KUDU, "sbin");
    if (!kuduBin.exists()) {
      kuduBin = new File(ABS_DIR_KUDU, "bin");
    }
    System.setProperty(SYSTEM_PROPERTY_KUDU_HOME_BIN, kuduBin.getAbsolutePath());
    File dataDir = new File(REL_DIR_KUDU);
    FileUtils.deleteDirectory(dataDir);
    dataDir.mkdirs();
    try {
      (miniKudu = new MiniKuduCluster.MiniKuduClusterBuilder().numMasters(KUDU_MSERVER_NUM).numTservers(KUDU_TSERVER_NUM)
        .defaultTimeoutMs(50000).build()).waitForTabletServers(KUDU_TSERVER_NUM);
    } catch (Exception exception) {
      throw new RuntimeException("Failed to start kudu processes, it is possible that a previous test was halted prior to "
        + "cleaning up the kudu processes it had spawned, if so this command "
        + "[ps aux | grep runtime-kudu | grep -v grep] will show those processes that should be killed", exception);
    }
    log(LOG, "start", time);
  }

  @Override
  public void clean() throws Exception {
    long time = log(LOG, "clean");
    KuduClient client = new KuduClientBuilder(miniKudu.getMasterAddresses()).build();
    try {
      for (String table : client.getTablesList().getTablesList()) {
        client.deleteTable(table);
      }
    } finally {
      client.shutdown();
    }
    log(LOG, "clean", time);
  }

  @Override
  public synchronized void state() throws Exception {
    long time = log(LOG, "state", true);
    KuduClient client = new KuduClientBuilder(miniKudu.getMasterAddresses()).build();
    try {
      log(LOG, "state", "tables:\n" + StringUtils.join(client.getTablesList().getTablesList().toArray(), "\n"), true);
    } finally {
      client.shutdown();
    }
    log(LOG, "state", time, true);
  }

  @Override
  public synchronized void stop() throws IOException {
    long time = log(LOG, "stop");
    if (miniKudu != null) {
      miniKudu.shutdown();
    }
    log(LOG, "stop", time);
  }

  public enum Runtime {
    CLUSTER_DEAMONS // Mini Kudu cluster, multi-process, heavy-weight
  }

}
