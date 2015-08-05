package com.cloudera.example.model;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class RecordFormat {

  public static final String TYPE_CSV = "csv";
  public static final String CODEC_NONE = "none";
  public static final String CONTAINER_TEXT = "text";

  public static Class<? extends FileInputFormat<RecordKey, Record>> getRecordInputFormat(String type, String codec,
      String container) {
    // TODO Provide implementation
    return null;
  }

}
