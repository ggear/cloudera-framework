package com.cloudera.framework.example.model.input;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.cloudera.framework.example.model.RecordKey;
import com.cloudera.framework.example.model.input.RecordTextCombineInputFormat.RecordReaderText;

/**
 * An {@link InputFormat} to act on text files, forming the {@link RecordKey
 * RecordKeys} and {@link Text} values
 */
public class RecordTextInputFormat extends FileInputFormat<RecordKey, Text> {

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  @Override
  public RecordReader<RecordKey, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
    return new RecordReaderText(split, context);
  }

}
