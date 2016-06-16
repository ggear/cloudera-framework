package com.cloudera.framework.testing.server.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.Arrays;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Test;

import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.SparkServer;

import scala.Tuple2;

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
  public void testSpark() throws InterruptedException, IOException {
    String dirInput = "/tmp/wordcount/input";
    String dirOutput = "/tmp/wordcount/output";
    String fileInput = new Path(dirInput, "file1.txt").toString();
    BufferedWriter writer = new BufferedWriter(
        new OutputStreamWriter(getDfsServer().getFileSystem().create(getDfsServer().getPath(fileInput))));
    writer.write("a a a a a\n");
    writer.write("b b\n");
    writer.close();
    getSparkServer().getContext().textFile(getDfsServer().getPathUri(fileInput)).cache().flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterable<String> call(String s) {
        return Arrays.asList(s.split(" "));
      }
    }).mapToPair(new PairFunction<String, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(String s) {
        return new Tuple2<>(s, 1);
      }
    }).reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer a, Integer b) {
        return a + b;
      }
    }).map(new Function<Tuple2<String, Integer>, String>() {
      @Override
      public String call(Tuple2<String, Integer> t) throws Exception {
        return t._1 + "\t" + t._2;
      }
    }).saveAsTextFile(getDfsServer().getPathUri(dirOutput));
    Path[] outputFiles = FileUtil.stat2Paths(getDfsServer().getFileSystem().listStatus(getDfsServer().getPath(dirOutput), new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return !path.getName().equals(FileOutputCommitter.SUCCEEDED_FILE_NAME);
      }
    }));
    assertEquals(1, outputFiles.length);
    InputStream in = getDfsServer().getFileSystem().open(outputFiles[0]);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    assertEquals("a\t5", reader.readLine());
    assertEquals("b\t2", reader.readLine());
    assertNull(reader.readLine());
    reader.close();
  }

  @Test
  public void testSparkAgain() throws InterruptedException, IOException {
    testSpark();
  }

}
