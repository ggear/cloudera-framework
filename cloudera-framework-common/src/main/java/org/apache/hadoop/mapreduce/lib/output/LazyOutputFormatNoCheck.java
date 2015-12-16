package org.apache.hadoop.mapreduce.lib.output;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;

/**
 * Override default behaviour to not check if directory exists or not
 */
public class LazyOutputFormatNoCheck<K, V> extends LazyOutputFormat<K, V> {

  public static void setOutputFormatClass(Job job,
      @SuppressWarnings("rawtypes") Class<? extends OutputFormat> theClass) {
    job.setOutputFormatClass(LazyOutputFormatNoCheck.class);
    job.getConfiguration().setClass(OUTPUT_FORMAT, theClass, OutputFormat.class);
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
  }

}
