package com.cloudera.framework.example.one.model.input;

import java.io.IOException;

import com.cloudera.framework.example.one.model.Record;
import com.cloudera.framework.example.one.model.RecordFactory;
import com.cloudera.framework.example.one.model.RecordKey;
import com.cloudera.framework.example.one.model.serde.RecordStringSerDe;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

/**
 * An {@link InputFormat} to act on sequence files of {@link RecordKey
 * RecordKeys} and CSV {@link Text Texts}, presented upstream as
 * {@link AvroGenericRecordWritable} wrapped {@link Record Records}
 */
public class RecordSequenceInputFormatCsv extends SequenceFileInputFormat<RecordKey, AvroGenericRecordWritable> {

  @Override
  public RecordReader<RecordKey, AvroGenericRecordWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
    throws IOException {
    return new RecordReaderSequenceCsv();
  }

  public static class RecordReaderSequenceCsv extends RecordTextReader {

    public RecordReaderSequenceCsv() throws IOException {
      super();
    }

    @Override
    public RecordReader<RecordKey, Text> getRecordReader(InputSplit split, TaskAttemptContext context, Integer index) {
      return new SequenceFileRecordReader<>();
    }

    @Override
    public RecordStringSerDe getRecordStringSerDe() throws IOException {
      return RecordFactory.getRecordStringSerDe(RecordFactory.RECORD_STRING_SERDE_CSV);
    }

  }

}
