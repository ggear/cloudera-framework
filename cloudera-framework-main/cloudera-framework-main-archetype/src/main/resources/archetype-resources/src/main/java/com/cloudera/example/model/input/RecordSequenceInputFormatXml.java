package com.cloudera.example.model.input;

import java.io.IOException;

import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

import com.cloudera.example.model.RecordFactory;
import com.cloudera.example.model.RecordKey;
import com.cloudera.example.model.serde.RecordStringSerDe;

public class RecordSequenceInputFormatXml extends SequenceFileInputFormat<RecordKey, AvroGenericRecordWritable> {

  @Override
  public RecordReader<RecordKey, AvroGenericRecordWritable> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    return new RecordReaderSequenceXml();
  }

  public static class RecordReaderSequenceXml extends RecordTextReader {

    public RecordReaderSequenceXml() throws IOException {
      super();
    }

    @Override
    public RecordReader<RecordKey, Text> getRecordReader(InputSplit split, TaskAttemptContext context, Integer index) {
      return new SequenceFileRecordReader<RecordKey, Text>();
    }

    @Override
    public RecordStringSerDe getRecordStringSerDe() throws IOException {
      return RecordFactory.getRecordStringSerDe(RecordFactory.RECORD_STRING_SERDE_XML);
    }

  }

}
