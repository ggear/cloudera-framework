package com.cloudera.framework.main.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Assert;
import org.junit.Test;

/**
 * MiniClusterDfsMrTest system test
 */
public class MiniClusterDfsMrTestTest extends MiniClusterDfsMrTest {

  /**
   * Test MR
   *
   * @throws Exception
   */
  @Test
  public void testMr() throws Exception {
    String dirInput = "/tmp/wordcount/input";
    String dirOutput = "/tmp/wordcount/output";
    String fileInput = new Path(dirInput, "file1.txt").toString();
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(this.getFileSystem().create(getPath(fileInput))));
    writer.write("a a a a a\n");
    writer.write("b b\n");
    writer.close();
    Job job = Job.getInstance(getConf());
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapperClass(MapClass.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    FileInputFormat.setInputPaths(job, getPath(dirInput));
    FileOutputFormat.setOutputPath(job, getPath(dirOutput));
    Assert.assertTrue(job.waitForCompletion(true));
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
   * Test MR
   *
   * @throws Exception
   */
  @Test
  public void testMrAgain() throws Exception {
    testMr();
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
  public void testDfsClean() throws Exception {
    Assert.assertFalse(getFileSystem().exists(new Path(getPathString("/some_dir/some_file"))));
  }

  /**
   * Test DFS path generation
   *
   * @throws Exception
   */
  @Test
  public void testDfsGetPath() throws Exception {
    Assert.assertEquals("/", getPathString(""));
    Assert.assertEquals("/", getPathString("/"));
    Assert.assertEquals("/", getPathString("//"));
    Assert.assertEquals("/tmp", getPathString("tmp"));
    Assert.assertEquals("/tmp", getPathString("/tmp"));
    Assert.assertEquals("/tmp", getPathString("//tmp"));
    Assert.assertEquals("/tmp", getPathString("///tmp"));
    Assert.assertEquals("/tmp//tmp", getPathString("///tmp//tmp"));
  }

  /**
   * Test DFS path URI generation
   *
   * @throws Exception
   */
  @Test
  public void testDfsGetPathUri() throws Exception {
    Assert.assertTrue(getPathUri("").matches("hdfs://localhost:[0-9]+/"));
    Assert.assertTrue(getPathUri("/").matches("hdfs://localhost:[0-9]+/"));
    Assert.assertTrue(getPathUri("//").matches("hdfs://localhost:[0-9]+/"));
    Assert.assertTrue(getPathUri("tmp").matches("hdfs://localhost:[0-9]+/tmp"));
    Assert.assertTrue(getPathUri("/tmp").matches("hdfs://localhost:[0-9]+/tmp"));
    Assert.assertTrue(getPathUri("//tmp").matches("hdfs://localhost:[0-9]+/tmp"));
    Assert.assertTrue(getPathUri("///tmp").matches("hdfs://localhost:[0-9]+/tmp"));
    Assert.assertTrue(getPathUri("///tmp//tmp").matches("hdfs://localhost:[0-9]+/tmp/tmp"));
  }

  private static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }

  }

  private static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      context.write(key, new IntWritable(sum));
    }

  }

}
