package com.cloudera.framework.example.one.model.input.hive;

import com.cloudera.framework.example.one.model.RecordKey;
import com.twitter.elephantbird.mapred.input.DeprecatedFileInputFormatWrapper;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.mapred.InputFormat;

/**
 * Provide an old API {@link InputFormat} facade over the new API
 * {@link org.apache.hadoop.mapreduce.InputFormat} implementation for Hive
 */
public class RecordSequenceInputFormatXml extends DeprecatedFileInputFormatWrapper<RecordKey, AvroGenericRecordWritable> {

  public RecordSequenceInputFormatXml() {
    super(new com.cloudera.framework.example.one.model.input.RecordSequenceInputFormatXml());
  }

}
