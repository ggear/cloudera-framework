package com.cloudera.framework.example.one.model.input;

import java.io.IOException;
import java.rmi.server.UID;

import com.cloudera.framework.example.one.model.Record;
import com.cloudera.framework.example.one.model.RecordKey;
import com.cloudera.framework.example.one.model.serde.RecordStringSerDe;
import com.cloudera.framework.example.one.model.serde.RecordStringSerDe.RecordStringDe;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * A parametrised {@link RecordReader} that can take in {@link Text} and form
 * {@link AvroGenericRecordWritable} wrapped {@link Record Records} based on the
 * {@link #getRecordReader} implementation
 */
public abstract class RecordTextReader extends RecordReader<RecordKey, AvroGenericRecordWritable> {

  private final Record record = new Record();
  private final RecordKey recordKey = new RecordKey();
  private final AvroGenericRecordWritable recordWriteable = new AvroGenericRecordWritable(record);
  private final UID recordReaderID = new UID();
  private RecordKey recordsKey;
  private RecordStringDe recordStringDe;
  private RecordReader<RecordKey, Text> recordReader;

  public RecordTextReader() throws IOException {
    recordReader = getRecordReader(null, null, null);
  }

  public RecordTextReader(InputSplit split, TaskAttemptContext context) throws IOException {
    recordReader = getRecordReader(split, context, null);
  }

  public RecordTextReader(InputSplit split, TaskAttemptContext context, Integer index) throws IOException {
    recordReader = getRecordReader(split, context, index);
  }

  public abstract RecordReader<RecordKey, Text> getRecordReader(InputSplit split, TaskAttemptContext context, Integer index)
    throws IOException;

  public abstract RecordStringSerDe getRecordStringSerDe() throws IOException;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    recordReader.initialize(split, context);
    recordWriteable.setFileSchema(Record.getClassSchema());
    recordWriteable.setRecordReaderID(recordReaderID);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (recordStringDe != null && recordStringDe.hasNext()) {
      recordStringDe.next(recordsKey);
      return true;
    }
    if (recordReader.nextKeyValue()) {
      recordsKey = recordReader.getCurrentKey();
      recordStringDe = getRecordStringSerDe().getDeserializer(recordKey, record, recordReader.getCurrentValue().toString());
      return nextKeyValue();
    }
    return false;
  }

  @Override
  public RecordKey getCurrentKey() throws IOException, InterruptedException {
    return recordKey;
  }

  @Override
  public AvroGenericRecordWritable getCurrentValue() throws IOException, InterruptedException {
    return recordWriteable;
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
