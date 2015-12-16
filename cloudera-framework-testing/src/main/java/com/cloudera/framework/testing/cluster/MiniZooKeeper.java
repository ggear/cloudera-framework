package com.cloudera.framework.testing.cluster;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import com.cloudera.framework.testing.BaseTest;
import com.cloudera.framework.testing.util.ClusterUtil;

/**
 * Provide a mini-ZooKeeper cluster
 */
public class MiniZooKeeper {

  public static final int ZOOKEEPER_TICK_MS = 2000;
  public static final int ZOOKEEPER_TIMEOUT_MS = 5000;

  private static final File DIR_DATA = new File(BaseTest.ABS_DIR_TARGET + "/test-zookeper");

  private int port;
  private ZooKeeperServer zooKeeper;
  private NIOServerCnxnFactory factory;

  public MiniZooKeeper() throws IOException, InterruptedException {
    port = ClusterUtil.getNextAvailablePort();
  }

  public void start() throws IOException, InterruptedException {
    FileUtils.deleteDirectory(DIR_DATA);
    zooKeeper = new ZooKeeperServer(DIR_DATA, DIR_DATA, ZOOKEEPER_TICK_MS);
    factory = new NIOServerCnxnFactory();
    factory.configure(new InetSocketAddress(ClusterUtil.SERVER_BIND_IP, port), 0);
    factory.startup(zooKeeper);
    final CountDownLatch connected = new CountDownLatch(1);
    ZooKeeper zookeeper = new ZooKeeper(getConnectString(), ZOOKEEPER_TIMEOUT_MS, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (event.getState() == KeeperState.SyncConnected) {
          connected.countDown();
        }
      }
    });
    try {
      if (!connected.await(ZOOKEEPER_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        throw new IOException("Could not connected to zookeeper");
      }
    } finally {
      zookeeper.close();
    }
  }

  public void stop() throws IOException {
    zooKeeper.shutdown();
    factory.shutdown();
  }

  public String getConnectString() {
    return ClusterUtil.SERVER_BIND_IP + ":" + port;
  }

}
