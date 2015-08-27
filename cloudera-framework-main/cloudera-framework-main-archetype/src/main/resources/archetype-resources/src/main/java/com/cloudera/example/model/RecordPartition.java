package com.cloudera.example.model;

/**
 * Define a {@link Record} partition
 */
public class RecordPartition {

  public static final String[] RECORD_COL_YEAR_MONTH = new String[] { //
      "my_timestamp_year", //
      "my_timestamp_month"//
  };
  public static final String[] RECORD_DDL_YEAR_MONTH = new String[] { //
      RECORD_COL_YEAR_MONTH[0] + " TINYINT", //
      RECORD_COL_YEAR_MONTH[1] + " TINYINT" //
  };

  public static final String[] BATCH_COL_NAME = new String[] { //
      "ingest_batch_name" //
  };
  public static final String[] BATCH_DDL_NAME = new String[] { //
      BATCH_COL_NAME[0] + " STRING" //
  };

  public static final String[] BATCH_COL_ID_START_FINISH = new String[] { //
      "ingest_batch_id", //
      "ingest_batch_start", //
      "ingest_batch_finish" //
  };
  public static final String[] BATCH_DDL_ID_START_FINISH = new String[] { //
      BATCH_COL_ID_START_FINISH[0] + " STRING", //
      BATCH_COL_ID_START_FINISH[1] + " STRING", //
      BATCH_COL_ID_START_FINISH[2] + " STRING" //
  };

}
