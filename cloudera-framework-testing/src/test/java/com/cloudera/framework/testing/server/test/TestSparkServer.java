package com.cloudera.framework.testing.server.test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;

import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.SparkServer;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

@SuppressWarnings("serial")
public abstract class TestSparkServer implements Serializable, TestConstants {

  public abstract DfsServer getDfsServer();

  public abstract SparkServer getSparkServer();

  @Test
  public void testGetDfsServer() {
    assertNotNull(getDfsServer());
    assertTrue(getDfsServer().isStarted());
  }

  @Test
  public void testGetSparkServer() {
    assertNotNull(getSparkServer());
    assertTrue(getSparkServer().isStarted());
  }

  @Test
  public void testSpark() throws IOException {
    int SAMPLES = 1000;
    String dirInput = "/tmp/pi/input";
    String fileInput = new Path(dirInput, "samples.txt").toString();
    BufferedWriter writer = new BufferedWriter(
      new OutputStreamWriter(getDfsServer().getFileSystem().create(getDfsServer().getPath(fileInput))));
    for (int i = 0; i < SAMPLES; i++) {
      writer.write(i + "\n");
    }
    writer.close();
    double piEstimate = getSparkServer().getContext().textFile(getDfsServer().getPathUri(fileInput)).cache().filter(i -> {
      double x = Math.random();
      double y = Math.random();
      return x * x + y * y < 1;
    }).count() * 4F / 1000;
    assertTrue(piEstimate > 2 && piEstimate < 5);
  }

  @Test
  public void testSparkAgain() throws IOException {
    testSpark();
  }

}
