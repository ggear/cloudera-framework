package com.cloudera.framework.testing.server;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZooKeeper {@link TestRule}
 */
public class ZooKeeperServer extends CdhServer<ZooKeeperServer, ZooKeeperServer.Runtime> {

  public static final int ZOOKEEPER_TICK_MS = 2000;
  public static final int ZOOKEEPER_TIMEOUT_MS = 5000;

  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperServer.class);

  private static ZooKeeperServer instance;

  private int port;
  private org.apache.zookeeper.server.ZooKeeperServer zooKeeper;
  private NIOServerCnxnFactory factory;

  private ZooKeeperServer(Runtime runtime) {
    super(runtime);
    port = CdhServer.getNextAvailablePort();
  }

  /**
   * Get instance with default runtime
   *
   * @return
   */
  public static synchronized ZooKeeperServer getInstance() {
    return getInstance(instance == null ? Runtime.CLUSTER_SERVER : instance.getRuntime());
  }

  /**
   * Get instance with specific <code>runtime</code>
   *
   * @return
   */
  public static synchronized ZooKeeperServer getInstance(Runtime runtime) {
    return instance == null ? instance = new ZooKeeperServer(runtime) : instance.assertRuntime(runtime);
  }

  /**
   * Get connect {@link String}
   *
   * @return
   */
  public String getConnectString() {
    return CdhServer.SERVER_BIND_IP + ":" + port;
  }

  @Override
  public int getIndex() {
    return 30;
  }

  @Override
  public synchronized void start() throws IOException, InterruptedException {
    long time = log(LOG, "start");
    FileUtils.deleteDirectory(new File(ABS_DIR_ZOOKEEPER));
    zooKeeper = new org.apache.zookeeper.server.ZooKeeperServer(new File(ABS_DIR_ZOOKEEPER), new File(ABS_DIR_ZOOKEEPER),
      ZOOKEEPER_TICK_MS);
    factory = new NIOServerCnxnFactory();
    factory.configure(new InetSocketAddress(CdhServer.SERVER_BIND_IP, port), 0);
    factory.startup(zooKeeper);
    final CountDownLatch connected = new CountDownLatch(1);
    ZooKeeper zooKeeper = new ZooKeeper(getConnectString(), ZOOKEEPER_TIMEOUT_MS, event -> {
      if (event.getState() == KeeperState.SyncConnected) {
        connected.countDown();
      }
    });
    try {
      if (!connected.await(ZOOKEEPER_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        throw new IOException("Could not connected to ZooKeeperServer");
      }
    } finally {
      zooKeeper.close();
    }
    log(LOG, "start", time);
  }

  @Override
  public synchronized void clean() throws Exception {
    long time = log(LOG, "clean");
    // TODO: Provide implementation, remove all state
    if (LOG.isWarnEnabled()) {
      LOG.warn(logPrefix() + " [clean] not implemented");
    }
    log(LOG, "clean", time);
  }

  @Override
  public synchronized void state() throws Exception {
    long time = log(LOG, "state", true);
    // TODO: Provide implementation, print state tree, like DfsServer.state()
    if (LOG.isWarnEnabled()) {
      LOG.warn(logPrefix() + " [state] not implemented");
    }
    log(LOG, "state", time, true);
  }

  @Override
  public synchronized void stop() {
    long time = log(LOG, "stop");
    if (factory != null) {
      factory.shutdown();
    }
    if (zooKeeper != null) {
      zooKeeper.shutdown();
    }
    log(LOG, "stop", time);
  }

  public enum Runtime {
    CLUSTER_SERVER // ZooKeeper servers, multi-threaded, heavy-weight
  }

}
