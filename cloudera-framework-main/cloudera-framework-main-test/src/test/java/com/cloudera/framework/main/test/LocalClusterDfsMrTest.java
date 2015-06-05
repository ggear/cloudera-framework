package com.cloudera.framework.main.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
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
 * Local-cluster unit tests
 */
public class LocalClusterDfsMrTest extends LocalClusterDfsMrBaseTest {

  /**
   * Test MR
   *
   * @throws Exception
   */
  @Test
  public void testMr() throws Exception {
    Path dirInput = new Path(getPathDfs("/tmp/wordcount/input"));
    Path dirOutput = new Path(getPathDfs("/tmp/wordcount/output"));
    Path hdfsFile = new Path(dirInput, "file1.txt");
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(this
        .getFileSystem().create(hdfsFile)));
    writer.write("a a a a a\n");
    writer.write("b b\n");
    writer.close();
    Job job = Job.getInstance(getConf());
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapperClass(MapClass.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    FileInputFormat.setInputPaths(job, dirInput);
    FileOutputFormat.setOutputPath(job, dirOutput);
    Assert.assertTrue(job.waitForCompletion(true));
    Path[] outputFiles = FileUtil.stat2Paths(getFileSystem().listStatus(
        dirOutput, new PathFilter() {
          @Override
          public boolean accept(Path path) {
            return !path.getName().equals(
                FileOutputCommitter.SUCCEEDED_FILE_NAME);
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
    Path dirInput = new Path(getPathDfs("/tmp/wordcount/input"));
    Path dirOutput = new Path(getPathDfs("/tmp/wordcount/output"));
    Path hdfsFile = new Path(dirInput, "file1.txt");
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(this
        .getFileSystem().create(hdfsFile)));
    writer.write("a a a a a\n");
    writer.write("b b\n");
    writer.close();
    Job job = Job.getInstance(getConf());
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapperClass(MapClass.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    FileInputFormat.setInputPaths(job, dirInput);
    FileOutputFormat.setOutputPath(job, dirOutput);
    Assert.assertTrue(job.waitForCompletion(true));
    Path[] outputFiles = FileUtil.stat2Paths(getFileSystem().listStatus(
        dirOutput, new PathFilter() {
          @Override
          public boolean accept(Path path) {
            return !path.getName().equals(
                FileOutputCommitter.SUCCEEDED_FILE_NAME);
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
    Assert
        .assertTrue(getFileSystem().mkdirs(new Path(getPathDfs("/some_dir"))));
    Assert.assertTrue(getFileSystem().createNewFile(
        new Path(getPathDfs("/some_dir/some_file"))));
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
    Assert.assertEquals(BaseTest.PATH_DFS_LOCAL, getPathDfs(""));
    Assert.assertEquals(BaseTest.PATH_DFS_LOCAL, getPathDfs("/"));
    Assert.assertEquals(BaseTest.PATH_DFS_LOCAL, getPathDfs("//"));
    Assert.assertEquals(BaseTest.PATH_DFS_LOCAL + "/tmp", getPathDfs("tmp"));
    Assert.assertEquals(BaseTest.PATH_DFS_LOCAL + "/tmp", getPathDfs("/tmp"));
    Assert.assertEquals(BaseTest.PATH_DFS_LOCAL + "/tmp", getPathDfs("//tmp"));
    Assert.assertEquals(BaseTest.PATH_DFS_LOCAL + "/tmp", getPathDfs("///tmp"));
    Assert.assertEquals(BaseTest.PATH_DFS_LOCAL + "/tmp/tmp",
        getPathDfs("///tmp//tmp"));
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

  private static class MapClass extends
      Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }

  }

  private static class Reduce extends
      Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      context.write(key, new IntWritable(sum));
    }

  }

}
