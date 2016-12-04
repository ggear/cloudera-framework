package com.cloudera.framework.example.model.input.hive;

import com.cloudera.framework.example.model.RecordKey;
import com.twitter.elephantbird.mapred.input.DeprecatedFileInputFormatWrapper;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.mapred.InputFormat;

/**
 * Provide an old API {@link InputFormat} facade over the new API
 * {@link org.apache.hadoop.mapreduce.InputFormat} implementation for Hive
 */
public class RecordTextInputFormatCsv extends DeprecatedFileInputFormatWrapper<RecordKey, AvroGenericRecordWritable> {

  public RecordTextInputFormatCsv() {
    super(new com.cloudera.framework.example.model.input.RecordTextInputFormatCsv());
  }

}
