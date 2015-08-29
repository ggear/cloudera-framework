package com.cloudera.example.model.input.hive;

import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;

import com.cloudera.example.model.RecordKey;
import com.twitter.elephantbird.mapred.input.DeprecatedFileInputFormatWrapper;

public class RecordTextInputFormatXml extends DeprecatedFileInputFormatWrapper<RecordKey, AvroGenericRecordWritable> {

  public RecordTextInputFormatXml() {
    super(new com.cloudera.example.model.input.RecordTextInputFormatXml());
  }

}
