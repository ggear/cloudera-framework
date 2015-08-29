package com.cloudera.example.model.input;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.cloudera.example.model.Record;
import com.cloudera.example.model.RecordFactory;
import com.cloudera.example.model.RecordKey;
import com.cloudera.example.model.serde.RecordStringSerDe;

/**
 * An abstract {@link InputFormat} to act on multiple text files, forming the
 * appropriate {@link RecordKey key} and {@link Record value} using a
 * {@link RecordStringSerDe} specified by {@link #getX()}.
 */
public class RecordTextInputFormatCsv extends FileInputFormat<RecordKey, AvroGenericRecordWritable> {

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  @Override
  public RecordReader<RecordKey, AvroGenericRecordWritable> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    return new RecordReaderTextCsv(split, context);
  }

  public static class RecordReaderTextCsv extends RecordTextReader {

    public RecordReaderTextCsv(InputSplit split, TaskAttemptContext context) throws IOException {
      super(split, context);
    }

    @Override
    public RecordReader<RecordKey, Text> getRecordReader(InputSplit split, TaskAttemptContext context, Integer index)
        throws IOException {
      return new RecordTextCombineInputFormat.RecordReaderText(split, context, index);
    }

    @Override
    public RecordStringSerDe getRecordStringSerDe() throws IOException {
      return RecordFactory.getRecordStringSerDe(RecordFactory.RECORD_STRING_SERDE_CSV);
    }

  }

}
