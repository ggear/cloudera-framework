package com.cloudera.framework.example.one;

/**
 * Test constants
 */
public interface TestConstants extends com.cloudera.framework.testing.TestConstants {

  String DS_DIR = REL_DIR_DATASET;
  String DS_MYDATASET = "mydataset";
  String DSS_MYDATASET_CSV = "csv";
  String DSS_MYDATASET_XML = "xml";

  String DSS_MYDATASET_PRISTINE_SINGLE = "pristine-single";
  String DSS_MYDATASET_PRISTINE_MULTI = "pristine-multi";
  String DSS_MYDATASET_CORRUPT_DIR = "corrupt-dir";
  String DSS_MYDATASET_CORRUPT_FILE = "corrupt-file";
  String DSS_MYDATASET_CORRUPT_STRUCT = "corrupt-struct";
  String DSS_MYDATASET_CORRUPT_TYPE = "corrupt-type";
  String DSS_MYDATASET_DUPLICATE_EVENT = "duplicate-event";
  String DSS_MYDATASET_DUPLICATE_RECORD = "duplicate-record";
  String DSS_MYDATASET_EMPTY_DIR = "empty-dir";
  String DSS_MYDATASET_EMPTY_FILE = "empty-file";

  String DDL_DIR = ABS_DIR_CLASSES + "/hive/schema/ddl";

}
