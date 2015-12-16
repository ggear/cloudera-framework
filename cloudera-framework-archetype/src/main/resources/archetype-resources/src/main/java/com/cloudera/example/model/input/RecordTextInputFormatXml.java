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
 * An {@link InputFormat} to act on text files of {@link RecordKey RecordKeys}
 * and XML {@link Text Texts}, presented upstream as
 * {@link AvroGenericRecordWritable} wrapped {@link Record Records}
 */
public class RecordTextInputFormatXml extends FileInputFormat<RecordKey, AvroGenericRecordWritable> {

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  @Override
  public RecordReader<RecordKey, AvroGenericRecordWritable> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    return new RecordReaderTextXml(split, context);
  }

  public static class RecordReaderTextXml extends RecordTextReader {

    public RecordReaderTextXml(InputSplit split, TaskAttemptContext context) throws IOException {
      super(split, context);
    }

    @Override
    public RecordReader<RecordKey, Text> getRecordReader(InputSplit split, TaskAttemptContext context, Integer index)
        throws IOException {
      return new RecordTextCombineInputFormat.RecordReaderText(split, context, index);
    }

    @Override
    public RecordStringSerDe getRecordStringSerDe() throws IOException {
      return RecordFactory.getRecordStringSerDe(RecordFactory.RECORD_STRING_SERDE_XML);
    }

  }

}
