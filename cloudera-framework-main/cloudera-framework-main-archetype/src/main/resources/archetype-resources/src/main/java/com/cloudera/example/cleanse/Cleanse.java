package com.cloudera.example.cleanse;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.example.Constants;
import com.cloudera.example.Constants.Counter;
import com.cloudera.framework.main.common.Driver;

/**
 * Dataset cleanse process
 */
public class Cleanse extends Driver {

  public static final String NAMED_OUTPUT_SEQUENCE = "sequence";

  private static final Logger LOG = LoggerFactory.getLogger(Cleanse.class);

  private Path inputPath;
  private Path outputPath;

  public Cleanse() {
    super();
  }

  public Cleanse(Configuration confguration) {
    super(confguration);
  }

  @Override
  public String description() {
    return "Clense my dataset";
  }

  @Override
  public String[] options() {
    return new String[] {};
  }

  @Override
  public String[] parameters() {
    return new String[] { "input-path", "output-path" };
  }

  @Override
  public void reset() {
    super.reset();
    for (Counter counter : Counter.values()) {
      incrementCounter(Cleanse.class.getCanonicalName(), counter, 0);
    }
  }

  @Override
  public int prepare(String... arguments) throws Exception {
    if (arguments == null || arguments.length != 2) {
      throw new Exception("Invalid number of arguments");
    }
    FileSystem hdfs = FileSystem.newInstance(getConf());
    inputPath = new Path(arguments[0]);
    if (!hdfs.exists(inputPath)) {
      throw new Exception("Input path [" + inputPath + "] does not exist");
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("Input path [" + inputPath + "] validated");
    }
    outputPath = new Path(arguments[1]);
    hdfs.mkdirs(outputPath.getParent());
    if (hdfs.exists(outputPath)) {
      throw new Exception("Output path [" + inputPath + "] already exists");
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("Output path [" + outputPath + "] validated");
    }
    return RETURN_SUCCESS;
  }

  @Override
  public int execute() throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName(getClass().getSimpleName());
    job.setJarByClass(Cleanse.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setMapperClass(CleanseMapper.class);
    job.setReducerClass(CleanseReducer.class);
    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);
    MultipleOutputs.addNamedOutput(job, NAMED_OUTPUT_SEQUENCE, SequenceFileOutputFormat.class, NullWritable.class,
        Text.class);
    boolean jobSuccess = job.waitForCompletion(LOG.isInfoEnabled());
    importCounters(job, new Counter[] { Counter.RECORDS_MALFORMED, Counter.RECORDS_DUPLICATE, Counter.RECORDS_CLEANSED,
        Counter.RECORDS });
    return jobSuccess ? RETURN_SUCCESS : RETURN_FAILURE_RUNTIME;
  }

  private static class CleanseMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
        throws IOException, InterruptedException {
      String valueString = value.toString();
      if (valueString.length() > 0) {
        context.getCounter(Counter.RECORDS).increment(1);
        String splitFileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        if (splitFileName.endsWith(Constants.DIR_DS_MYDATASET_RAW_SOURCE_TEXT_COMMA_SUFFIX)) {
        } else if (splitFileName.endsWith(Constants.DIR_DS_MYDATASET_RAW_SOURCE_TEXT_COMMA_SUFFIX)) {
        }
        context.write(new IntWritable(valueString.hashCode()), value);
      }
    }

  }

  private static class CleanseReducer extends Reducer<IntWritable, Text, NullWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> value,
        Reducer<IntWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
      context.write(null, value.iterator().next());
    }

  }

}
