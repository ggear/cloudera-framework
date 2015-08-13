package com.cloudera.example.model.input;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

import com.cloudera.example.model.Record;
import com.cloudera.example.model.RecordFactory;
import com.cloudera.example.model.RecordKey;
import com.cloudera.example.model.serde.RecordStringSerDe;

public class RecordSequenceInputFormatCsv extends SequenceFileInputFormat<RecordKey, Record> {

  @Override
  public RecordReader<RecordKey, Record> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException {
    return new RecordReaderSequenceCsv();
  }

  public static class RecordReaderSequenceCsv extends RecordTextReader {

    public RecordReaderSequenceCsv() throws IOException {
      super();
    }

    @Override
    public RecordReader<RecordKey, Text> getRecordReader(CombineFileSplit split, TaskAttemptContext context,
        Integer index) {
      return new SequenceFileRecordReader<RecordKey, Text>();
    }

    @Override
    public RecordStringSerDe getRecordStringSerDe() throws IOException {
      return RecordFactory.getRecordStringSerDe(RecordFactory.RECORD_STRING_SERDE_CSV);
    }

  }

}
