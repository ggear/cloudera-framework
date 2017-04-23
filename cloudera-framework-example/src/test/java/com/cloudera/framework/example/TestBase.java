package com.cloudera.framework.example;

import com.cloudera.framework.example.model.RecordCounter;
import com.cloudera.framework.example.process.Partition;
import com.cloudera.framework.example.process.Stage;
import com.cloudera.framework.testing.TestMetaData;
import com.google.common.collect.ImmutableMap;

/**
 *
 */
public abstract class TestBase implements Constants, TestConstants {

  public final TestMetaData testMetaDataCsvPristine = TestMetaData.getInstance() //
    .dataSetSourceDirs(DS_DIR) //
    .dataSetNames(DS_MYDATASET) //
    .dataSetSubsets(new String[][]{{DSS_MYDATASET_CSV}}) //
    .dataSetLabels(new String[][][]{{{DSS_MYDATASET_PRISTINE_SINGLE}}}) //
    .dataSetDestinationDirs(DIR_ABS_MYDS_RAW_CANONICAL_CSV) //
    .asserts( //
      ImmutableMap.of( //
        Stage.class.getCanonicalName(),
        ImmutableMap.of( //
          RecordCounter.FILES, 1L, //
          RecordCounter.FILES_CANONICAL, 1L, //
          RecordCounter.FILES_DUPLICATE, 0L, //
          RecordCounter.FILES_MALFORMED, 0L //
        ), //
        Partition.class.getCanonicalName(),
        ImmutableMap.of( //
          RecordCounter.RECORDS, 1L, //
          RecordCounter.RECORDS_CANONICAL, 1L, //
          RecordCounter.RECORDS_DUPLICATE, 0L, //
          RecordCounter.RECORDS_MALFORMED, 0L //
        ), //
        com.cloudera.framework.example.process.Cleanse.class.getCanonicalName(),
        ImmutableMap.of( //
          RecordCounter.RECORDS, 1L, //
          RecordCounter.RECORDS_CANONICAL, 1L, //
          RecordCounter.RECORDS_DUPLICATE, 0L, //
          RecordCounter.RECORDS_MALFORMED, 0L //
        )), //
      ImmutableMap.of( //
        Stage.class.getCanonicalName(),
        ImmutableMap.of(//
          RecordCounter.FILES, 0L, //
          RecordCounter.FILES_CANONICAL, 0L, //
          RecordCounter.FILES_DUPLICATE, 0L, //
          RecordCounter.FILES_MALFORMED, 0L //
        ), //
        Partition.class.getCanonicalName(),
        ImmutableMap.of( //
          RecordCounter.RECORDS, 0L, //
          RecordCounter.RECORDS_CANONICAL, 0L, //
          RecordCounter.RECORDS_DUPLICATE, 0L, //
          RecordCounter.RECORDS_MALFORMED, 0L //
        ), //
        com.cloudera.framework.example.process.Cleanse.class.getCanonicalName(),
        ImmutableMap.of( //
          RecordCounter.RECORDS, 0L, //
          RecordCounter.RECORDS_CANONICAL, 0L, //
          RecordCounter.RECORDS_DUPLICATE, 0L, //
          RecordCounter.RECORDS_MALFORMED, 0L //
        )));

  public final TestMetaData testMetaDataXmlPristine = TestMetaData.getInstance() //
    .dataSetSourceDirs(DS_DIR) //
    .dataSetNames(DS_MYDATASET) //
    .dataSetSubsets(new String[][]{{DSS_MYDATASET_XML}}) //
    .dataSetLabels(new String[][][]{{{DSS_MYDATASET_PRISTINE_SINGLE}}}) //
    .dataSetDestinationDirs(DIR_ABS_MYDS_RAW_CANONICAL_XML) //
    .asserts( //
      ImmutableMap.of( //
        Stage.class.getCanonicalName(),
        ImmutableMap.of( //
          RecordCounter.FILES, 1L, //
          RecordCounter.FILES_CANONICAL, 1L, //
          RecordCounter.FILES_DUPLICATE, 0L, //
          RecordCounter.FILES_MALFORMED, 0L //
        ), //
        Partition.class.getCanonicalName(),
        ImmutableMap.of( //
          RecordCounter.RECORDS, 1L, //
          RecordCounter.RECORDS_CANONICAL, 1L, //
          RecordCounter.RECORDS_DUPLICATE, 0L, //
          RecordCounter.RECORDS_MALFORMED, 0L //
        ), //
        com.cloudera.framework.example.process.Cleanse.class.getCanonicalName(),
        ImmutableMap.of( //
          RecordCounter.RECORDS, 1L, //
          RecordCounter.RECORDS_CANONICAL, 1L, //
          RecordCounter.RECORDS_DUPLICATE, 0L, //
          RecordCounter.RECORDS_MALFORMED, 0L //
        )), //
      ImmutableMap.of( //
        Stage.class.getCanonicalName(),
        ImmutableMap.of(//
          RecordCounter.FILES, 0L, //
          RecordCounter.FILES_CANONICAL, 0L, //
          RecordCounter.FILES_DUPLICATE, 0L, //
          RecordCounter.FILES_MALFORMED, 0L //
        ), //
        Partition.class.getCanonicalName(),
        ImmutableMap.of( //
          RecordCounter.RECORDS, 0L, //
          RecordCounter.RECORDS_CANONICAL, 0L, //
          RecordCounter.RECORDS_DUPLICATE, 0L, //
          RecordCounter.RECORDS_MALFORMED, 0L //
        ), //
        com.cloudera.framework.example.process.Cleanse.class.getCanonicalName(),
        ImmutableMap.of( //
          RecordCounter.RECORDS, 0L, //
          RecordCounter.RECORDS_CANONICAL, 0L, //
          RecordCounter.RECORDS_DUPLICATE, 0L, //
          RecordCounter.RECORDS_MALFORMED, 0L //
        )));

  public final TestMetaData testMetaDataAll = TestMetaData.getInstance() //
    .dataSetSourceDirs(DS_DIR, DS_DIR) //
    .dataSetNames(DS_MYDATASET, DS_MYDATASET) //
    .dataSetSubsets(new String[][]{{DSS_MYDATASET_CSV}, {DSS_MYDATASET_XML}}) //
    .dataSetLabels(new String[][][]{{{null},}, {{null},}}) //
    .dataSetDestinationDirs(DIR_ABS_MYDS_RAW_CANONICAL_CSV, DIR_ABS_MYDS_RAW_CANONICAL_XML) //
    .asserts( //
      ImmutableMap.of( //
        Stage.class.getCanonicalName(),
        ImmutableMap.of( //
          RecordCounter.FILES, 89L, //
          RecordCounter.FILES_CANONICAL, 56L, //
          RecordCounter.FILES_DUPLICATE, 0L, //
          RecordCounter.FILES_MALFORMED, 33L //
        ), //
        Partition.class.getCanonicalName(),
        ImmutableMap.of(//
          RecordCounter.RECORDS, 493L, //
          RecordCounter.RECORDS_CANONICAL, 332L, //
          RecordCounter.RECORDS_DUPLICATE, 140L, //
          RecordCounter.RECORDS_MALFORMED, 21L //
        ), //
        com.cloudera.framework.example.process.Cleanse.class.getCanonicalName(),
        ImmutableMap.of(//
          RecordCounter.RECORDS, 332L, //
          RecordCounter.RECORDS_CANONICAL, 332L, //
          RecordCounter.RECORDS_DUPLICATE, 0L, //
          RecordCounter.RECORDS_MALFORMED, 0L //
        )), //
      ImmutableMap.of( //
        Stage.class.getCanonicalName(),
        ImmutableMap.of(//
          RecordCounter.FILES, 0L, //
          RecordCounter.FILES_CANONICAL, 0L, //
          RecordCounter.FILES_DUPLICATE, 0L, //
          RecordCounter.FILES_MALFORMED, 0L //
        ), //
        Partition.class.getCanonicalName(),
        ImmutableMap.of( //
          RecordCounter.RECORDS, 0L, //
          RecordCounter.RECORDS_CANONICAL, 0L, //
          RecordCounter.RECORDS_DUPLICATE, 0L, //
          RecordCounter.RECORDS_MALFORMED, 0L //
        ), //
        com.cloudera.framework.example.process.Cleanse.class.getCanonicalName(),
        ImmutableMap.of( //
          RecordCounter.RECORDS, 0L, //
          RecordCounter.RECORDS_CANONICAL, 0L, //
          RecordCounter.RECORDS_DUPLICATE, 0L, //
          RecordCounter.RECORDS_MALFORMED, 0L //
        )));

}
