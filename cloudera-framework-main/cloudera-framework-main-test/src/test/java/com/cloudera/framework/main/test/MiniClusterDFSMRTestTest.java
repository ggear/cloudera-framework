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

public class MiniClusterDFSMRTestTest extends MiniClusterDFSMRTest {

  @Test
  public void testPathHDFS() throws Exception {
    Assert.assertEquals("", getPathHDFS(""));
    Assert.assertEquals("/", getPathHDFS("/"));
    Assert.assertEquals("//", getPathHDFS("//"));
    Assert.assertEquals("tmp", getPathHDFS("tmp"));
    Assert.assertEquals("/tmp", getPathHDFS("/tmp"));
    Assert.assertEquals("//tmp", getPathHDFS("//tmp"));
    Assert.assertEquals("///tmp", getPathHDFS("///tmp"));
    Assert.assertEquals("///tmp//tmp", getPathHDFS("///tmp//tmp"));
  }

  @Test(expected = Exception.class)
  public void testPathLocal() throws Exception {
    Assert.assertNull(getPathLocal(""));
  }

  @Test
  public void testFileSystemMkDir() throws Exception {
    Assert.assertFalse(getFileSystem().exists(new Path("/some_dir/some_file")));
    Assert.assertTrue(getFileSystem().mkdirs(new Path("/some_dir")));
    Assert.assertTrue(getFileSystem().createNewFile(
        new Path("/some_dir/some_file")));
    Assert.assertTrue(getFileSystem().exists(new Path("/some_dir/some_file")));
  }

  @Test
  public void testFileSystemClean() throws Exception {
    Assert.assertFalse(getFileSystem().exists(new Path("/some_dir/some_file")));
  }

  @Test
  public void testMapReduce() throws Exception {
    Path dirInput = new Path(getPathHDFS("/tmp/wordcount/input"));
    Path dirOutput = new Path(getPathHDFS("/tmp/wordcount/output"));
    Path hdfsFile = new Path(dirInput, "file1.txt");
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(this
        .getFileSystem().create(hdfsFile)));
    writer.write("a a a a a\n");
    writer.write("b b\n");
    writer.close();
    Job job = Job.getInstance(getFileSystem().getConf());
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

  public static class MapClass extends
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

  public static class Reduce extends
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
