package com.cloudera.framework.test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;
import org.junit.Test;

import com.cloudera.framework.testing.LocalClusterDfsMrZkKafkaTest;
import com.cloudera.framework.testing.cluster.MiniKafka;
import com.cloudera.framework.testing.cluster.MiniZooKeeper;

/**
 * LocalClusterDfsMrZkKafkaTest system test
 */
public class LocalClusterDfsMrZkKafkaTestTest extends LocalClusterDfsMrZkKafkaTest {

  @Test
  public void testZookeeper() throws IOException, InterruptedException, KeeperException {
    final CountDownLatch connected = new CountDownLatch(1);
    ZooKeeper zooKeeper = new ZooKeeper(getZooKeeper().getConnectString(), MiniZooKeeper.ZOOKEEPER_TIMEOUT_MS,
        new Watcher() {
          @Override
          public void process(WatchedEvent event) {
            if (event.getState() == KeeperState.SyncConnected) {
              connected.countDown();
            }
          }
        });
    Assert.assertTrue(connected.await(MiniZooKeeper.ZOOKEEPER_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    String node = "/mytestnode";
    final CountDownLatch created = new CountDownLatch(1);
    zooKeeper.exists(node, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (event.getType() == EventType.NodeCreated) {
          created.countDown();
        }
      }
    });
    zooKeeper.create(node, node.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    Assert.assertTrue(created.await(MiniZooKeeper.ZOOKEEPER_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    zooKeeper.delete(node, -1);
    zooKeeper.close();
  }

  @Test
  public void testKafka() throws InterruptedException, IOException, ExecutionException {
    String topic = "mytesttopic";
    MiniKafka kafka = getKafka();
    kafka.create(topic);
    kafka.send(topic, "my-key", "my-value");

    // TODO Add new consumer due with Kafka 0.9.0

    kafka.delete(topic);
  }

}
