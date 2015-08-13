package com.cloudera.example.model.input;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import com.cloudera.example.model.Record;
import com.cloudera.example.model.RecordFactory;
import com.cloudera.example.model.RecordKey;
import com.cloudera.example.model.serde.RecordStringSerDe;

/**
 * An abstract {@link InputFormat} to act on multiple text files, forming the
 * appropriate {@link RecordKey key} and {@link Record value} using a
 * {@link RecordStringSerDe} specified by {@link #getX()}.
 */
public abstract class RecordTextInputFormatXml extends CombineFileInputFormat<RecordKey, Record> {

  public RecordTextInputFormatXml() {
    setMaxSplitSize(RecordTextInputFormat.SPLIT_SIZE_BYTES_MAX);
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  @Override
  public RecordReader<RecordKey, Record> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException {
    return new CombineFileRecordReader<RecordKey, Record>((CombineFileSplit) split, context, RecordReaderTextXml.class);
  }

  public static class RecordReaderTextXml extends RecordTextReader {

    public RecordReaderTextXml(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException {
      super();
    }

    @Override
    public RecordReader<RecordKey, Text> getRecordReader(CombineFileSplit split, TaskAttemptContext context,
        Integer index) throws IOException {
      return new RecordTextInputFormat.RecordReaderText(split, context, index);
    }

    @Override
    public RecordStringSerDe getRecordStringSerDe() throws IOException {
      return RecordFactory.getRecordStringSerDe(RecordFactory.RECORD_STRING_SERDE_XML);
    }

  }

}
