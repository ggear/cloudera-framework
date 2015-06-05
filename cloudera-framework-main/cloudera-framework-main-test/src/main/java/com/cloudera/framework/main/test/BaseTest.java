package com.cloudera.framework.main.test;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.LogManager;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import uk.org.lidalia.sysoutslf4j.context.SysOutOverSLF4J;

public abstract class BaseTest {

  private static Logger LOG = LoggerFactory.getLogger(BaseTest.class);

  public static String DIR_WORKING = "target";
  public static String DIR_FS_LOCAL = "test-fs-local";
  public static String DIR_HDFS_LOCAL = "test-hdfs-local";
  public static String DIR_HDFS_MINICLUSTER = "test-hdfs-minicluster";

  public static String PATH_FS_LOCAL = DIR_WORKING + "/" + DIR_FS_LOCAL;
  public static String PATH_HDFS_LOCAL = DIR_WORKING + "/" + DIR_HDFS_LOCAL;
  public static String PATH_HDFS_MINICLUSTER = DIR_WORKING + "/"
      + DIR_HDFS_MINICLUSTER;

  public static String PATH_LOCAL_WORKING_DIR = new File(".").getAbsolutePath();
  public static String PATH_LOCAL_WORKING_DIR_TARGET = PATH_LOCAL_WORKING_DIR
      + "/" + DIR_WORKING;
  public static String PATH_LOCAL_WORKING_DIR_TARGET_HDFS_LOCAL = PATH_LOCAL_WORKING_DIR_TARGET
      + "/" + DIR_HDFS_LOCAL;
  public static String PATH_LOCAL_WORKING_DIR_TARGET_HDFS_MINICLUSTER = PATH_LOCAL_WORKING_DIR_TARGET
      + "/" + DIR_HDFS_MINICLUSTER;

  public abstract Configuration getConf() throws Exception;

  public abstract FileSystem getFileSystem() throws Exception;

  public abstract String getPathLocal(String pathRelativeToModuleRoot)
      throws Exception;

  public abstract String getPathHDFS(String pathRelativeToHDFSRoot)
      throws Exception;

  @BeforeClass
  public static void setUpSystem() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Test harness [setUpSystem] starting");
    }
    SysOutOverSLF4J.sendSystemOutAndErrToSLF4J();
    LogManager.getLogManager().reset();
    SLF4JBridgeHandler.install();
    java.util.logging.Logger.getGlobal().setLevel(Level.OFF);
    System.setProperty("java.security.krb5.realm", "CDHCLUSTER.com");
    System.setProperty("java.security.krb5.kdc", "kdc.cdhcluster.com");
    System.setProperty("java.security.krb5.conf", "/dev/null");
    System.setProperty("dir.working", PATH_LOCAL_WORKING_DIR);
    System.setProperty("dir.working.target", PATH_LOCAL_WORKING_DIR_TARGET);
    System.setProperty("dir.working.target.hdfs",
        PATH_LOCAL_WORKING_DIR_TARGET_HDFS_LOCAL);
    System.setProperty("test.build.data",
        PATH_LOCAL_WORKING_DIR_TARGET_HDFS_MINICLUSTER);
    System.setProperty("dir.working.target.derby", PATH_LOCAL_WORKING_DIR
        + "/target/derby");
    System.setProperty("dir.working.target.derby.db",
        System.getProperty("dir.working.target.derby") + "/db");
    System.setProperty("derby.stream.error.file",
        System.getProperty("dir.working.target.derby") + "/derby.log");
    File derbyDir = new File(System.getProperty("dir.working.target.derby.db"));
    try {
      FileUtils.deleteDirectory(derbyDir);
      derbyDir.mkdirs();
    } catch (IOException e) {
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Test harness [setUpSystem] finished");
    }
  }

  @Before
  public void setUpFileSystem() throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Test harness [setUpFileSystem] starting");
    }
    FileSystem fileSystem = getFileSystem();
    if (fileSystem != null) {
      String rootDir = "/";
      String tmpDir = "/tmp";
      String userDir = "/user";
      String userHiveDir = userDir + "/hive";
      String userIdDir = userDir + "/" + System.getProperty("user.name");
      String userIdWorkingDir = userIdDir + "/target";
      String userIdWorkingDirPrefix = "MiniMRCluster_";
      Path rootPath = new Path(getPathHDFS(rootDir));
      Path tmpPath = new Path(getPathHDFS(tmpDir));
      Path userPath = new Path(getPathHDFS(userDir));
      Path userHivePath = new Path(getPathHDFS(userHiveDir));
      Path userIdPath = new Path(getPathHDFS(userIdDir));
      Path userIdWorkingPath = new Path(getPathHDFS(userIdWorkingDir));
      if (fileSystem.exists(rootPath)) {
        for (FileStatus fileStatus : fileSystem.listStatus(rootPath)) {
          if (!fileStatus.getPath().getName().equals(userPath.getName())) {
            fileSystem.delete(fileStatus.getPath(), true);
          }
        }
      }
      if (fileSystem.exists(userPath)) {
        for (FileStatus fileStatus : fileSystem.listStatus(userPath)) {
          if (!fileStatus.getPath().getName().equals(userIdPath.getName())) {
            fileSystem.delete(fileStatus.getPath(), true);
          }
        }
      }
      if (fileSystem.exists(userIdPath)) {
        for (FileStatus fileStatus : fileSystem.listStatus(userIdPath)) {
          if (!fileStatus.getPath().getName()
              .equals(userIdWorkingPath.getName())) {
            fileSystem.delete(fileStatus.getPath(), true);
          }
        }
      }
      if (fileSystem.exists(userIdWorkingPath)) {
        for (FileStatus fileStatus : fileSystem.listStatus(userIdWorkingPath)) {
          if (!fileStatus.getPath().getName()
              .startsWith(userIdWorkingDirPrefix)) {
            fileSystem.delete(fileStatus.getPath(), true);
          }
        }
      }
      fileSystem.mkdirs(tmpPath, new FsPermission(FsAction.ALL, FsAction.ALL,
          FsAction.ALL));
      fileSystem.mkdirs(userHivePath, new FsPermission(FsAction.ALL,
          FsAction.ALL, FsAction.ALL));
      fileSystem.mkdirs(userIdPath, new FsPermission(FsAction.ALL,
          FsAction.ALL, FsAction.ALL));
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Test harness [setUpFileSystem] finished");
    }
  }

  protected static String stripLeadingSlashes(String string) {
    int indexAfterLeadingSlash = 0;
    while (indexAfterLeadingSlash < string.length()
        && string.charAt(indexAfterLeadingSlash) == '/')
      ++indexAfterLeadingSlash;
    return indexAfterLeadingSlash == 0 ? string : string.substring(
        indexAfterLeadingSlash, string.length());
  }

}
