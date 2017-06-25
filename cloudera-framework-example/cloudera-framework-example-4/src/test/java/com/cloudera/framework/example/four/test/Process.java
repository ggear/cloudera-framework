package com.cloudera.framework.example.four.test;

import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.KafkaServer;
import com.cloudera.framework.testing.server.KuduServer;
import com.cloudera.framework.testing.server.SparkServer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Test process
 */
@RunWith(TestRunner.class)
public class Process implements TestConstants {

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static final KuduServer kuduServer = KuduServer.getInstance();

  @ClassRule
  public static final KafkaServer kafkaServer = KafkaServer.getInstance();

  @ClassRule
  public static final SparkServer sparkServer = SparkServer.getInstance();

  /**
   * Test process
   */
  @Test
  public void testProcess() throws Exception {
  }

}
