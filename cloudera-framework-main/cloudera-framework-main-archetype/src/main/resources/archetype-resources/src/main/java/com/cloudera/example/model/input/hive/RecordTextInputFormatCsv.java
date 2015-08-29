package com.cloudera.example.model.input.hive;

import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;

import com.cloudera.example.model.RecordKey;
import com.twitter.elephantbird.mapred.input.DeprecatedFileInputFormatWrapper;

public class RecordTextInputFormatCsv extends DeprecatedFileInputFormatWrapper<RecordKey, AvroGenericRecordWritable> {

  public RecordTextInputFormatCsv() {
    super(new com.cloudera.example.model.input.RecordTextInputFormatCsv());
  }

}
