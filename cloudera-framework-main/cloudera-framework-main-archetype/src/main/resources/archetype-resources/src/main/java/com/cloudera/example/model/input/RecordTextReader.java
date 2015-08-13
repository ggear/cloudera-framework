package com.cloudera.example.model.input;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import com.cloudera.example.model.Record;
import com.cloudera.example.model.RecordKey;
import com.cloudera.example.model.serde.RecordStringSerDe;
import com.cloudera.example.model.serde.RecordStringSerDe.RecordStringDe;

public abstract class RecordTextReader extends RecordReader<RecordKey, Record> {

  public abstract RecordReader<RecordKey, Text> getRecordReader(CombineFileSplit split, TaskAttemptContext context,
      Integer index) throws IOException;

  public abstract RecordStringSerDe getRecordStringSerDe() throws IOException;

  private Record record;
  private RecordKey recordKey;
  private RecordKey recordsKey;
  private RecordStringDe recordStringDe;
  private RecordReader<RecordKey, Text> recordReader;

  public RecordTextReader() throws IOException {
    recordReader = getRecordReader(null, null, null);
  }

  public RecordTextReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException {
    recordReader = getRecordReader(split, context, index);
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    recordReader.initialize(split, context);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (recordStringDe != null && recordStringDe.hasNext()) {
      recordStringDe.next(recordsKey);
      recordKey = recordStringDe.getKey();
      record = recordStringDe.getRecord();
      return true;
    }
    if (recordReader.nextKeyValue()) {
      recordsKey = recordReader.getCurrentKey();
      recordStringDe = getRecordStringSerDe().getDeserialiser(recordReader.getCurrentValue().toString());
      return nextKeyValue();
    }
    return false;
  }

  @Override
  public RecordKey getCurrentKey() throws IOException, InterruptedException {
    return recordKey;
  }

  @Override
  public Record getCurrentValue() throws IOException, InterruptedException {
    return record;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return recordStringDe == null ? recordReader.getProgress() : recordStringDe.hasNext() ? 0F : 1F;
  }

  @Override
  public void close() throws IOException {
    recordReader.close();
  }

}
