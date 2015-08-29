package com.cloudera.example.model;

import com.twitter.elephantbird.mapred.input.DeprecatedInputFormatValueCopier;

public class RecordCopier implements DeprecatedInputFormatValueCopier<Record> {

  @Override
  public void copyValue(Record oldValue, Record newValue) {
    newValue.setIngestId(oldValue.getIngestId());
    newValue.setIngestTimestamp(oldValue.getIngestTimestamp());
    newValue.setIngestBatch(oldValue.getIngestBatch());
    newValue.setMyTimestamp(oldValue.getMyTimestamp());
    newValue.setMyInteger(oldValue.getMyInteger());
    newValue.setMyDouble(oldValue.getMyDouble());
    newValue.setMyBoolean(oldValue.getMyBoolean());
    newValue.setMyString(oldValue.getMyString());
  }

}
