package com.cloudera.framework.testing;

import java.io.File;

/**
 * Test constants
 */
public interface TestConstants {

  // Directories
  String DIR_SOURCE = "src/main";
  String DIR_SCALA = "scala";
  String DIR_PYTHON = "python";
  String DIR_TARGET = "target";
  String DIR_DATASET = "data";
  String DIR_DATA = "test-data";
  String DIR_CLASSES = "classes";
  String DIR_CLASSES_TEST = "test-classes";
  String DIR_DERBY = "test-derby";
  String DIR_DERBY_DB = "test-db";
  String DIR_DERBY_LOG = "derby.log";
  String DIR_SCRIPT = "test-script";
  String DIR_DFS = "test-dfs";
  String DIR_DFS_TMP = "test-dfs-tmp";
  String DIR_DFS_LOCAL = "test-dfs-local";
  String DIR_KUDU = "test-kudu";
  String DIR_HIVE = "test-hive";
  String DIR_KAFKA = "test-kafka";
  String DIR_FLUME = "test-flume";
  String DIR_ZOOKEEPER = "test-zookeper";
  String DIR_SOURCE_SCRIPT = "src/main/script";

  // Runtime directories
  String DIR_RUNTIME_KUDU = "runtime-kudu";
  String DIR_RUNTIME_MQTT = "runtime-mqtt";
  String DIR_RUNTIME_MR = "MiniMRCluster_";
  String DIR_RUNTIME_PYTHON = "test-python";

  // Relative directories
  String REL_DIR_DATA = DIR_TARGET + "/" + DIR_DATA;
  String REL_DIR_CLASSES = DIR_TARGET + "/" + DIR_CLASSES;
  String REL_DIR_CLASSES_TEST = DIR_TARGET + "/" + DIR_CLASSES_TEST;
  String REL_DIR_DATASET = REL_DIR_CLASSES_TEST + "/" + DIR_DATASET;
  String REL_DIR_DFS_LOCAL = DIR_TARGET + "/" + DIR_DFS_LOCAL;
  String REL_DIR_SCRIPT = DIR_TARGET + "/" + DIR_SCRIPT;
  String REL_DIR_DFS = DIR_TARGET + "/" + DIR_DFS;
  String REL_DIR_KUDU = DIR_TARGET + "/" + DIR_KUDU;

  // Absolute directories
  String ABS_DIR_WORKING = new File(".").getAbsolutePath().substring(0, new File(".").getAbsolutePath().length() - 2);
  String ABS_DIR_SOURCE = ABS_DIR_WORKING + "/" + DIR_SOURCE;
  String ABS_DIR_SCALA_SRC = ABS_DIR_SOURCE + "/" + DIR_SCALA;
  String ABS_DIR_PYTHON_SRC = ABS_DIR_SOURCE + "/" + DIR_PYTHON;
  String ABS_DIR_TARGET = ABS_DIR_WORKING + "/" + DIR_TARGET;
  String ABS_DIR_CLASSES = ABS_DIR_TARGET + "/" + DIR_CLASSES;
  String ABS_DIR_CLASSES_TEST = ABS_DIR_TARGET + "/" + DIR_CLASSES_TEST;
  String ABS_DIR_DATA = ABS_DIR_TARGET + "/" + DIR_DATA;
  String ABS_DIR_DERBY = ABS_DIR_TARGET + "/" + DIR_DERBY;
  String ABS_DIR_DERBY_DB = ABS_DIR_DERBY + "/" + DIR_DERBY_DB;
  String ABS_DIR_DERBY_LOG = ABS_DIR_DERBY + "/" + DIR_DERBY_LOG;
  String ABS_DIR_DFS = ABS_DIR_TARGET + "/" + DIR_DFS;
  String ABS_DIR_DFS_TMP = ABS_DIR_TARGET + "/" + DIR_DFS_TMP;
  String ABS_DIR_DFS_LOCAL = ABS_DIR_TARGET + "/" + DIR_DFS_LOCAL;
  String ABS_DIR_KUDU = ABS_DIR_TARGET + "/" + DIR_RUNTIME_KUDU;
  String ABS_DIR_MQTT = ABS_DIR_TARGET + "/" + DIR_RUNTIME_MQTT;
  String ABS_DIR_HIVE = ABS_DIR_TARGET + "/" + DIR_HIVE;
  String ABS_DIR_KAFKA = ABS_DIR_TARGET + "/" + DIR_KAFKA;
  String ABS_DIR_FLUME = ABS_DIR_TARGET + "/" + DIR_FLUME;
  String ABS_DIR_ZOOKEEPER = ABS_DIR_TARGET + "/" + DIR_ZOOKEEPER;
  String ABS_DIR_PYTHON = ABS_DIR_CLASSES + "/" + DIR_PYTHON;
  String ABS_DIR_PYTHON_BIN = ABS_DIR_TARGET + "/" + DIR_RUNTIME_PYTHON + "/" + DIR_PYTHON + "/bin/" + DIR_PYTHON;
  String ABS_DIR_HIVE_QUERY = ABS_DIR_CLASSES + "/hive/query";
  String ABS_DIR_HIVE_SCHEMA = ABS_DIR_CLASSES + "/hive/schema";
  String ABS_DIR_HIVE_REFRESH = ABS_DIR_CLASSES + "/hive/refresh";

}
