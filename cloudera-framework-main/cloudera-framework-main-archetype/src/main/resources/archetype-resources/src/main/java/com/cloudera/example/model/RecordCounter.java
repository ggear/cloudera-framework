package com.cloudera.example.model;

/**
 * Counters specific to {@link Record records}
 */
public enum RecordCounter {

  FILES, //
  FILES_PARTITIONED, //
  FILES_MALFORMED, //

  RECORDS, //
  RECORDS_CLEANSED, //
  RECORDS_DUPLICATE, //
  RECORDS_MALFORMED;

}
