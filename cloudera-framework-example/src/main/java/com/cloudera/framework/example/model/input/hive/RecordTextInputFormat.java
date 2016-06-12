package com.cloudera.framework.example.model.input.hive;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;

import com.cloudera.framework.example.model.RecordKey;
import com.twitter.elephantbird.mapred.input.DeprecatedFileInputFormatWrapper;

/**
 * Provide an old API {@link InputFormat} facade over the new API
 * {@link org.apache.hadoop.mapreduce.InputFormat} implementation for Hive
 */
public class RecordTextInputFormat extends DeprecatedFileInputFormatWrapper<RecordKey, Text> {

  public RecordTextInputFormat() {
    super(new com.cloudera.framework.example.model.input.RecordTextInputFormat());
  }

}
