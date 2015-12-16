package com.cloudera.framework.test;

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

import com.cloudera.framework.testing.BaseTest;
import com.cloudera.framework.testing.LocalClusterDfsSparkTest;

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
    String dirInput = "/tmp/wordcount/input";
    String dirOutput = "/tmp/wordcount/output";
    String fileInput = new Path(dirInput, "file1.txt").toString();
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(this.getFileSystem().create(getPath(fileInput))));
    writer.write("a a a a a\n");
    writer.write("b b\n");
    writer.close();
    getContext().textFile(getPathUri(fileInput)).cache().flatMap(new FlatMapFunction<String, String>() {
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
    }).saveAsTextFile(getPathUri(dirOutput));
    Path[] outputFiles = FileUtil.stat2Paths(getFileSystem().listStatus(getPath(dirOutput), new PathFilter() {
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
    testSpark();
  }

  /**
   * Test DFS mkdir and file touch
   *
   * @throws IOException
   */
  @Test
  public void testDfsMkDir() throws Exception {
    Assert.assertFalse(getFileSystem().exists(new Path(getPathString("/some_dir/some_file"))));
    Assert.assertTrue(getFileSystem().mkdirs(new Path(getPathString("/some_dir"))));
    Assert.assertTrue(getFileSystem().createNewFile(new Path(getPathString("/some_dir/some_file"))));
    Assert.assertTrue(getFileSystem().exists(new Path(getPathString("/some_dir/some_file"))));
  }

  /**
   * Test DFS is clean
   *
   * @throws IOException
   */
  @Test
  public void testDfsClean() throws IOException {
    Assert.assertFalse(new File(getPathString("/some_dir/some_file")).exists());
  }

  /**
   * Test DFS path generation
   *
   * @throws Exception
   */
  @Test
  public void testDfsGetPath() throws Exception {
    Assert.assertEquals(BaseTest.REL_DIR_DFS_LOCAL, getPathString(""));
    Assert.assertEquals(BaseTest.REL_DIR_DFS_LOCAL, getPathString("/"));
    Assert.assertEquals(BaseTest.REL_DIR_DFS_LOCAL, getPathString("//"));
    Assert.assertEquals(BaseTest.REL_DIR_DFS_LOCAL + "/tmp", getPathString("tmp"));
    Assert.assertEquals(BaseTest.REL_DIR_DFS_LOCAL + "/tmp", getPathString("/tmp"));
    Assert.assertEquals(BaseTest.REL_DIR_DFS_LOCAL + "/tmp", getPathString("//tmp"));
    Assert.assertEquals(BaseTest.REL_DIR_DFS_LOCAL + "/tmp", getPathString("///tmp"));
    Assert.assertEquals(BaseTest.REL_DIR_DFS_LOCAL + "/tmp/tmp", getPathString("///tmp//tmp"));
  }

  /**
   * Test DFS path generation
   *
   * @throws Exception
   */
  @Test
  public void testDfsGetPathUri() throws Exception {
    Assert.assertTrue(getPathUri("").matches("file:/.*" + BaseTest.DIR_DFS_LOCAL));
    Assert.assertTrue(getPathUri("/").matches("file:/.*" + BaseTest.DIR_DFS_LOCAL));
    Assert.assertTrue(getPathUri("//").matches("file:/.*" + BaseTest.DIR_DFS_LOCAL));
    Assert.assertTrue(getPathUri("tmp").matches("file:/.*" + BaseTest.DIR_DFS_LOCAL + "/tmp"));
    Assert.assertTrue(getPathUri("/tmp").matches("file:/.*" + BaseTest.DIR_DFS_LOCAL + "/tmp"));
    Assert.assertTrue(getPathUri("//tmp").matches("file:/.*" + BaseTest.DIR_DFS_LOCAL + "/tmp"));
    Assert.assertTrue(getPathUri("///tmp").matches("file:/.*" + BaseTest.DIR_DFS_LOCAL + "/tmp"));
    Assert.assertTrue(getPathUri("///tmp//tmp").matches("file:/.*" + BaseTest.DIR_DFS_LOCAL + "/tmp/tmp"));
  }

}
