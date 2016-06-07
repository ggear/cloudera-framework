package com.cloudera.framework.testing;

import java.io.File;

/**
 * Test constants
 */
public interface TestConstants {

  // Directories
  public static final String DIR_TARGET = "target";
  public static final String DIR_DATASET = "data";
  public static final String DIR_DATA = "test-data";
  public static final String DIR_CLASSES = "test-classes";
  public static final String DIR_DERBY = "test-derby";
  public static final String DIR_DERBY_DB = "test-db";
  public static final String DIR_DERBY_LOG = "derby.log";
  public static final String DIR_DFS = "test-dfs";
  public static final String DIR_DFS_TMP = "test-dfs-tmp";
  public static final String DIR_DFS_LOCAL = "test-dfs-local";
  public static final String DIR_KUDU = "test-kudu";
  public static final String DIR_HIVE = "test-hive";
  public static final String DIR_KAFKA = "test-kafka";
  public static final String DIR_ZOOKEEPER = "test-zookeper";

  public static final String DIR_RUNTIME_KUDU = "runtime-kudu";
  public static final String DIR_RUNTIME_MR = "MiniMRCluster_";

  // Relative directories
  public static final String REL_DIR_DATA = DIR_TARGET + "/" + DIR_DATA;
  public static final String REL_DIR_CLASSES = DIR_TARGET + "/" + DIR_CLASSES;
  public static final String REL_DIR_DATASET = REL_DIR_CLASSES + "/" + DIR_DATASET;
  public static final String REL_DIR_DFS_LOCAL = DIR_TARGET + "/" + DIR_DFS_LOCAL;
  public static final String REL_DIR_DFS = DIR_TARGET + "/" + DIR_DFS;
  public static final String REL_DIR_KUDU = DIR_TARGET + "/" + DIR_KUDU;

  // Absolute directories
  public static final String ABS_DIR_WORKING = new File(".").getAbsolutePath().substring(0,
      new File(".").getAbsolutePath().length() - 2);
  public static final String ABS_DIR_TARGET = ABS_DIR_WORKING + "/" + DIR_TARGET;
  public static final String ABS_DIR_DATA = ABS_DIR_TARGET + "/" + DIR_DATA;
  public static final String ABS_DIR_DERBY = ABS_DIR_TARGET + "/" + DIR_DERBY;
  public static final String ABS_DIR_DERBY_DB = ABS_DIR_DERBY + "/" + DIR_DERBY_DB;
  public static final String ABS_DIR_DERBY_LOG = ABS_DIR_DERBY + "/" + DIR_DERBY_LOG;
  public static final String ABS_DIR_DFS = ABS_DIR_TARGET + "/" + DIR_DFS;
  public static final String ABS_DIR_DFS_TMP = ABS_DIR_TARGET + "/" + DIR_DFS_TMP;
  public static final String ABS_DIR_DFS_LOCAL = ABS_DIR_TARGET + "/" + DIR_DFS_LOCAL;
  public static final String ABS_DIR_KUDU = ABS_DIR_TARGET + "/" + DIR_RUNTIME_KUDU;
  public static final String ABS_DIR_HIVE = ABS_DIR_TARGET + "/" + DIR_HIVE;
  public static final String ABS_DIR_KAFKA = ABS_DIR_TARGET + "/" + DIR_KAFKA;
  public static final String ABS_DIR_ZOOKEEPER = ABS_DIR_TARGET + "/" + DIR_ZOOKEEPER;

}
