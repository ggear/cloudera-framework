package com.cloudera.framework.testing.server.test;

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
import java.util.StringTokenizer;

import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.MrServer;
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
import org.junit.Test;

public abstract class TestMrServer implements TestConstants {

  public abstract DfsServer getDfsServer();

  public abstract MrServer getMrServer();

  @Test
  public void testGetDfsServer() {
    assertNotNull(getDfsServer());
    assertTrue(getDfsServer().isStarted());
  }

  @Test
  public void testGetMrServer() {
    assertNotNull(getMrServer());
    assertTrue(getMrServer().isStarted());
  }

  @Test
  public void testMr() throws InterruptedException, IOException, ClassNotFoundException {
    String dirInput = "/tmp/wordcount/input";
    String dirOutput = "/tmp/wordcount/output";
    String fileInput = new Path(dirInput, "file1.txt").toString();
    BufferedWriter writer = new BufferedWriter(
      new OutputStreamWriter(getDfsServer().getFileSystem().create(getDfsServer().getPath(fileInput))));
    writer.write("a a a a a\n");
    writer.write("b b\n");
    writer.close();
    Job job = Job.getInstance(getMrServer().getConf());
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapperClass(MapClass.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    FileInputFormat.setInputPaths(job, getDfsServer().getPathUri(dirInput));
    FileOutputFormat.setOutputPath(job, new Path(getDfsServer().getPathUri(dirOutput)));
    assertTrue(job.waitForCompletion(true));
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
  public void testMrAgain() throws InterruptedException, IOException, ClassNotFoundException {
    testMr();
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
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      context.write(key, new IntWritable(sum));
    }

  }

}
