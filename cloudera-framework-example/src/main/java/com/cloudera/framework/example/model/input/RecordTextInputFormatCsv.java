package com.cloudera.framework.example.model.input;

import java.io.IOException;

import com.cloudera.framework.example.model.Record;
import com.cloudera.framework.example.model.RecordFactory;
import com.cloudera.framework.example.model.RecordKey;
import com.cloudera.framework.example.model.serde.RecordStringSerDe;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * An {@link InputFormat} to act on text files of {@link RecordKey RecordKeys}
 * and CSV {@link Text Texts}, presented upstream as
 * {@link AvroGenericRecordWritable} wrapped {@link Record Records}
 */
public class RecordTextInputFormatCsv extends FileInputFormat<RecordKey, AvroGenericRecordWritable> {

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  @Override
  public RecordReader<RecordKey, AvroGenericRecordWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
    throws IOException {
    return new RecordReaderTextCsv(split, context);
  }

  public static class RecordReaderTextCsv extends RecordTextReader {

    public RecordReaderTextCsv(InputSplit split, TaskAttemptContext context) throws IOException {
      super(split, context);
    }

    @Override
    public RecordReader<RecordKey, Text> getRecordReader(InputSplit split, TaskAttemptContext context, Integer index) throws IOException {
      return new RecordTextCombineInputFormat.RecordReaderText(split, context, index);
    }

    @Override
    public RecordStringSerDe getRecordStringSerDe() throws IOException {
      return RecordFactory.getRecordStringSerDe(RecordFactory.RECORD_STRING_SERDE_CSV);
    }

  }

}
