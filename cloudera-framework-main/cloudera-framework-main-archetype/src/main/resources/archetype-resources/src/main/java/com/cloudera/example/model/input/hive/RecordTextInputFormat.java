package com.cloudera.example.model.input.hive;

import org.apache.hadoop.io.Text;

import com.cloudera.example.model.RecordKey;
import com.twitter.elephantbird.mapred.input.DeprecatedFileInputFormatWrapper;

public class RecordTextInputFormat extends DeprecatedFileInputFormatWrapper<RecordKey, Text> {

  public RecordTextInputFormat() {
    super(new com.cloudera.example.model.input.RecordTextInputFormat());
  }

}
