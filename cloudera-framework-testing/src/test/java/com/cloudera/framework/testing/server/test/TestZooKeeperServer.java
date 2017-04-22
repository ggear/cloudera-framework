package com.cloudera.framework.testing.server.test;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.ZooKeeperServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(TestRunner.class)
public class TestZooKeeperServer implements TestConstants {

  @ClassRule
  public static final ZooKeeperServer zooKeeperServer = ZooKeeperServer.getInstance();

  @Test
  public void testZookeeper() throws IOException, InterruptedException, KeeperException {
    final CountDownLatch connected = new CountDownLatch(1);
    ZooKeeper zooKeeper = new ZooKeeper(zooKeeperServer.getConnectString(), ZooKeeperServer.ZOOKEEPER_TIMEOUT_MS, event -> {
      if (event.getState() == KeeperState.SyncConnected) {
        connected.countDown();
      }
    });
    assertTrue(connected.await(ZooKeeperServer.ZOOKEEPER_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    String node = "/mytestnode";
    final CountDownLatch created = new CountDownLatch(1);
    zooKeeper.exists(node, event -> {
      if (event.getType() == EventType.NodeCreated) {
        created.countDown();
      }
    });
    zooKeeper.create(node, node.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    assertTrue(created.await(ZooKeeperServer.ZOOKEEPER_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    zooKeeper.close();
  }

  @Test
  @Ignore // TODO: Remove when ZooKeeperServer.clean() is implemented and all
  // state is flushed between test methods and this test can pass
  public void testZookeeperAgain() throws IOException, InterruptedException, KeeperException {
    testZookeeper();
  }

}
