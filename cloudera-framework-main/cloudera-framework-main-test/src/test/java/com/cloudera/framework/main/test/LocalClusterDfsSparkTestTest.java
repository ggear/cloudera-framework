package com.cloudera.framework.main.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Arrays;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Assert;
import org.junit.Test;

import scala.Tuple2;

/**
 * LocalClusterDfsSparkTest system test
 */
@SuppressWarnings("serial")
public class LocalClusterDfsSparkTestTest extends LocalClusterDfsSparkTest {

  /**
   * Test Spark
   *
   * @throws Exception
   */
  @Test
  public void testSpark() throws Exception {
    Path dirInput = new Path(getPathDfs("/tmp/wordcount/input"));
    Path dirOutput = new Path(getPathDfs("/tmp/wordcount/output"));
    Path fileInput = new Path(dirInput, "file1.txt");
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(this.getFileSystem().create(fileInput)));
    writer.write("a a a a a\n");
    writer.write("b b\n");
    writer.close();
    getContext().textFile(fileInput.toString()).cache().flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterable<String> call(String s) {
        return Arrays.asList(s.split(" "));
      }
    }).mapToPair(new PairFunction<String, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(String s) {
        return new Tuple2<String, Integer>(s, 1);
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
    }).saveAsTextFile(dirOutput.toString());
    Path[] outputFiles = FileUtil.stat2Paths(getFileSystem().listStatus(dirOutput, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return !path.getName().equals(FileOutputCommitter.SUCCEEDED_FILE_NAME);
      }
    }));
    Assert.assertEquals(1, outputFiles.length);
    InputStream in = getFileSystem().open(outputFiles[0]);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    Assert.assertEquals("a\t5", reader.readLine());
    Assert.assertEquals("b\t2", reader.readLine());
    Assert.assertNull(reader.readLine());
    reader.close();
  }

  /**
   * Test Spark
   *
   * @throws Exception
   */
  @Test
  public void testSparkAgain() throws Exception {
    Path dirInput = new Path(getPathDfs("/tmp/wordcount/input"));
    Path dirOutput = new Path(getPathDfs("/tmp/wordcount/output"));
    Path fileInput = new Path(dirInput, "file1.txt");
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(this.getFileSystem().create(fileInput)));
    writer.write("a a a a a\n");
    writer.write("b b\n");
    writer.close();
    getContext().textFile(fileInput.toString()).cache().flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterable<String> call(String s) {
        return Arrays.asList(s.split(" "));
      }
    }).mapToPair(new PairFunction<String, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(String s) {
        return new Tuple2<String, Integer>(s, 1);
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
    }).saveAsTextFile(dirOutput.toString());
    Path[] outputFiles = FileUtil.stat2Paths(getFileSystem().listStatus(dirOutput, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return !path.getName().equals(FileOutputCommitter.SUCCEEDED_FILE_NAME);
      }
    }));
    Assert.assertEquals(1, outputFiles.length);
    InputStream in = getFileSystem().open(outputFiles[0]);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    Assert.assertEquals("a\t5", reader.readLine());
    Assert.assertEquals("b\t2", reader.readLine());
    Assert.assertNull(reader.readLine());
    reader.close();
  }

  /**
   * Test DFS mkdir and file touch
   *
   * @throws IOException
   */
  @Test
  public void testDfsMkDir() throws Exception {
    Assert.assertFalse(new File(getPathLocal("/some_dir/some_file")).exists());
    Assert.assertTrue(getFileSystem().mkdirs(new Path(getPathDfs("/some_dir"))));
    Assert.assertTrue(getFileSystem().createNewFile(new Path(getPathDfs("/some_dir/some_file"))));
    Assert.assertFalse(new File(getPathLocal("/some_dir/some_file")).exists());
  }

  /**
   * Test DFS is clean
   *
   * @throws IOException
   */
  @Test
  public void testDfsClean() throws IOException {
    Assert.assertFalse(new File(getPathLocal("/some_dir/some_file")).exists());
  }

  /**
   * Test path generation relative to DFS root
   *
   * @throws Exception
   */
  @Test
  public void testPathDfs() throws Exception {
    Assert.assertEquals(BaseTest.REL_DIR_DFS_LOCAL, getPathDfs(""));
    Assert.assertEquals(BaseTest.REL_DIR_DFS_LOCAL, getPathDfs("/"));
    Assert.assertEquals(BaseTest.REL_DIR_DFS_LOCAL, getPathDfs("//"));
    Assert.assertEquals(BaseTest.REL_DIR_DFS_LOCAL + "/tmp", getPathDfs("tmp"));
    Assert.assertEquals(BaseTest.REL_DIR_DFS_LOCAL + "/tmp", getPathDfs("/tmp"));
    Assert.assertEquals(BaseTest.REL_DIR_DFS_LOCAL + "/tmp", getPathDfs("//tmp"));
    Assert.assertEquals(BaseTest.REL_DIR_DFS_LOCAL + "/tmp", getPathDfs("///tmp"));
    Assert.assertEquals(BaseTest.REL_DIR_DFS_LOCAL + "/tmp/tmp", getPathDfs("///tmp//tmp"));
  }

  /**
   * Test path generation relative to module root
   *
   * @throws Exception
   */
  @Test
  public void testPathLocal() throws Exception {
    String localDir = new File(".").getAbsolutePath();
    localDir = localDir.substring(0, localDir.length() - 2);
    Assert.assertEquals(localDir, getPathLocal(""));
    Assert.assertEquals(localDir, getPathLocal("/"));
    Assert.assertEquals(localDir, getPathLocal("//"));
    Assert.assertEquals(localDir + "/tmp", getPathLocal("tmp"));
    Assert.assertEquals(localDir + "/tmp", getPathLocal("/tmp"));
    Assert.assertEquals(localDir + "/tmp", getPathLocal("//tmp"));
    Assert.assertEquals(localDir + "/tmp", getPathLocal("///tmp"));
    Assert.assertEquals(localDir + "/tmp/tmp", getPathLocal("///tmp//tmp"));
  }

}
