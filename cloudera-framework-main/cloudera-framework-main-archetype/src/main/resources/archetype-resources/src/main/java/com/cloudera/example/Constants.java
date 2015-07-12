package com.cloudera.example;

/**
 * Constants
 */
public interface Constants {

  // Directory structure
  // my-dataset/raw/source/text/comma-delim/none
  // my-dataset/raw/source/text/tab-delim/none
  // my-dataset/processed/cleansed/parquet/dict-encoded/snappy
  // my-dataset/processed/cleansed/sequence/control-delim/none
  // my-dataset/processed/duplicate/sequence/control-delim/none
  // my-dataset/processed/malformed/sequence/control-delim/none

  public static final String DIR_DS_MYDATASET = "/my-dataset";
  public static final String DIR_DS_MYDATASET_RAW = DIR_DS_MYDATASET + "/raw";
  public static final String DIR_DS_MYDATASET_RAW_SOURCE_TEXT = DIR_DS_MYDATASET_RAW + "/source/text";
  public static final String DIR_DS_MYDATASET_RAW_SOURCE_TEXT_TAB = DIR_DS_MYDATASET_RAW_SOURCE_TEXT
      + "/tab-delim/none";
  public static final String DIR_DS_MYDATASET_RAW_SOURCE_TEXT_TAB_SUFFIX = ".tsv";
  public static final String DIR_DS_MYDATASET_RAW_SOURCE_TEXT_COMMA = DIR_DS_MYDATASET_RAW_SOURCE_TEXT
      + "/comma-delim/none";
  public static final String DIR_DS_MYDATASET_RAW_SOURCE_TEXT_COMMA_SUFFIX = ".csv";
  public static final String DIR_DS_MYDATASET_PROCESSED = DIR_DS_MYDATASET + "/processed";
  public static final String DIR_DS_MYDATASET_PROCESSED_CLEANSED = DIR_DS_MYDATASET_PROCESSED + "/cleansed";
  public static final String DIR_DS_MYDATASET_PROCESSED_DUPLICATE = DIR_DS_MYDATASET_PROCESSED + "/duplicate";
  public static final String DIR_DS_MYDATASET_PROCESSED_MALFORMED = DIR_DS_MYDATASET_PROCESSED + "/malformed";

  public enum Counter {

    RECORDS, //
    RECORDS_CLEANSED("cleansed"), //
    RECORDS_DUPLICATE("duplicate"), //
    RECORDS_MALFORMED("malformed");

    private String path = "";

    Counter() {
    }

    Counter(String path) {
      this.path = path;
    }

    public String getPath() {
      return path;
    }

  }

}
