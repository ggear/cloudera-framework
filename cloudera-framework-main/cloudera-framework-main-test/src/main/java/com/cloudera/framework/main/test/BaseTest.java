package com.cloudera.framework.main.test;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import parquet.Log;
import parquet.hadoop.ParquetOutputFormat;
import uk.org.lidalia.sysoutslf4j.context.SysOutOverSLF4J;

/**
 * Base class for all unit tests, not intended for direct extension
 */
public abstract class BaseTest {

  // Directories
  public static final String DIR_TARGET = "target";
  public static final String DIR_DATA = "test-data";
  public static final String DIR_CLASSES = "test-classes";
  public static final String DIR_FS_LOCAL = "test-fs-local";
  public static final String DIR_DFS_LOCAL = "test-hdfs-local";
  public static final String DIR_DFS_MINICLUSTER = "test-hdfs-minicluster";
  public static final String DIR_MINICLUSTER_PREFIX = "MiniMRCluster_";

  // Relative directories
  public static final String REL_DIR_DATA = DIR_TARGET + "/" + DIR_DATA;
  public static final String REL_DIR_CLASSES = DIR_TARGET + "/" + DIR_CLASSES;
  public static final String REL_DIR_FS_LOCAL = DIR_TARGET + "/" + DIR_FS_LOCAL;
  public static final String REL_DIR_DFS_LOCAL = DIR_TARGET + "/"
      + DIR_DFS_LOCAL;
  public static final String REL_DIR_DFS_MINICLUSTER = DIR_TARGET + "/"
      + DIR_DFS_MINICLUSTER;

  // Absolute directories
  public static final String ABS_DIR_WORKING = new File(".").getAbsolutePath();
  public static final String ABS_DIR_TARGET = ABS_DIR_WORKING + "/"
      + DIR_TARGET;
  public static final String ABS_DIR_DATA = ABS_DIR_WORKING + "/" + DIR_DATA;
  public static final String ABS_DIR_DFS_LOCAL = ABS_DIR_TARGET + "/"
      + DIR_DFS_LOCAL;
  public static final String ABS_DIR_DFS_MINICLUSTER = ABS_DIR_TARGET + "/"
      + DIR_DFS_MINICLUSTER;

  /**
   * Get the {@link Configuration} for clients of this test
   *
   * @return
   */
  public abstract Configuration getConf();

  /**
   * Get the {@link FileSystem} for clients of this test
   *
   * @return
   */
  public abstract FileSystem getFileSystem();

  /**
   * Get the absolute local file system path from a local file system path
   * relative to the module root
   *
   * @param path
   * @return
   */
  public static String getPathLocal(String path) {
    String pathRelativeToModuleRootSansLeadingSlashes = stripLeadingSlashes(path);
    return pathRelativeToModuleRootSansLeadingSlashes.equals("") ? ABS_DIR_WORKING
        .length() < 2 ? "/" : ABS_DIR_WORKING.substring(0,
        ABS_DIR_WORKING.length() - 2) : new Path(ABS_DIR_WORKING,
        pathRelativeToModuleRootSansLeadingSlashes).toUri().toString();
  }

  /**
   * Get the relative local file system path from a local file system path
   * relative to the DFS root
   *
   * @param path
   * @return
   */
  public String getPathDfs(String path) {
    return path;
  }

  /**
   * Get a local file listing relative to the module root
   *
   * @param path
   * @return
   */
  public static List<File> listFilesLocal(String path, String... paths) {
    return listFilesLocal(path, true, paths);
  }

  /**
   * Get a DFS file listing relative to the DFS root
   *
   * @param path
   * @return
   */
  public List<Path> listFilesDfs(String path) throws IllegalArgumentException,
      IOException {
    List<Path> paths = new ArrayList<Path>();
    try {
      RemoteIterator<LocatedFileStatus> locatedFileStatuses = getFileSystem()
          .listFiles(new Path(getPathDfs(path)), true);
      while (locatedFileStatuses.hasNext()) {
        paths.add(locatedFileStatuses.next().getPath());
      }
    } catch (FileNotFoundException fileNotFoundException) {
      // ignore
    }
    return paths;
  }

  /**
   * Copy files from a local directory relative to to the module root, to a DFS
   * directory relative to the DFS root, matching specific directory labels
   *
   * @param sourcePath
   *          the source path relative to the module root
   * @param destinationPath
   *          the destination path relative to the DFS root
   * @param sourcePaths
   *          optional list of up to 3 nested directories to include, if not
   *          specified all directories at that level will be included
   * @return local files that have been copied
   */
  public List<File> copyFromLocalDir(String sourcePath, String destinationPath,
      String... sourcePaths) throws IllegalArgumentException, IOException {
    long time = debugMessageHeader(LOG, "copyFromLocalDir");
    List<File> files = new ArrayList<File>();
    String sourcePathGlob = ((sourcePaths.length == 0 ? "*" : sourcePaths[0])
        + "/" + (sourcePaths.length <= 1 ? "*" : sourcePaths[1]) + "/" + (sourcePaths.length <= 2 ? "*"
        : sourcePaths[2])).replace(ABS_DIR_WORKING, ".");
    getFileSystem().mkdirs(new Path(getPathDfs(destinationPath)));
    for (File file : listFilesLocal(sourcePath, false, sourcePaths)) {
      copyFromLocalFile(Arrays.asList(new Path(file.getPath())), new Path(
          getPathDfs(destinationPath)));
      if (file.isFile()) {
        files.add(file);
      } else {
        files.addAll(FileUtils.listFiles(file, TrueFileFilter.INSTANCE,
            TrueFileFilter.INSTANCE));
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug(LOG_PREFIX + " [copyFromLocalDir] copied ["
            + file.getParentFile().getParentFile().getParentFile().getName()
            + "/" + file.getParentFile().getParentFile().getName() + "/"
            + file.getParentFile().getName() + "/" + file.getName()
            + (file.isDirectory() ? "/" : "") + "] of glob [" + sourcePathGlob
            + "/*] to [" + destinationPath + "]");
      }
    }
    if (files.isEmpty()) {
      throw new IllegalArgumentException("Cloud not find files with path ["
          + sourcePathGlob + "]");
    }
    debugMessageFooter(LOG, "copyFromLocalDir", time);
    return files;
  }

  @BeforeClass
  public static void setUpSystem() throws Exception {
    long time = debugMessageHeader(LOG, "setUpSystem");
    Log.getLog(ParquetOutputFormat.class);
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
    SysOutOverSLF4J.sendSystemOutAndErrToSLF4J();
    System.setProperty("java.security.krb5.realm", "CDHCLUSTER.com");
    System.setProperty("java.security.krb5.kdc", "kdc.cdhcluster.com");
    System.setProperty("java.security.krb5.conf", "/dev/null");
    System.setProperty("dir.working", ABS_DIR_WORKING);
    System.setProperty("dir.working.target", ABS_DIR_TARGET);
    System.setProperty("dir.working.target.hdfs", ABS_DIR_DFS_LOCAL);
    System.setProperty("test.build.data", ABS_DIR_DFS_MINICLUSTER);
    System.setProperty("dir.working.target.derby", ABS_DIR_WORKING
        + "/target/derby");
    System.setProperty("dir.working.target.derby.db",
        System.getProperty("dir.working.target.derby") + "/db");
    System.setProperty("derby.stream.error.file",
        System.getProperty("dir.working.target.derby") + "/derby.log");
    for (File file : new File(ABS_DIR_TARGET).listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.isDirectory()
            && pathname.getName().startsWith(DIR_MINICLUSTER_PREFIX);
      }
    })) {
      FileUtils.deleteDirectory(file);
    }
    File derbyDir = new File(System.getProperty("dir.working.target.derby.db"));
    try {
      FileUtils.deleteDirectory(derbyDir);
      derbyDir.mkdirs();
    } catch (IOException e) {
    }
    debugMessageFooter(LOG, "setUpSystem", time);
  }

  @Before
  public void setUpFileSystem() throws Exception {
    long time = debugMessageHeader(LOG, "setUpFileSystem");
    FileSystem fileSystem = getFileSystem();
    if (fileSystem != null) {
      String rootDir = "/";
      String tmpDir = "/tmp";
      String userDir = "/user";
      String userHiveDir = userDir + "/hive";
      String userIdDir = userDir + "/" + System.getProperty("user.name");
      String userIdWorkingDir = userIdDir + "/target";
      String userIdWorkingDirPrefix = DIR_MINICLUSTER_PREFIX;
      Path rootPath = new Path(getPathDfs(rootDir));
      Path tmpPath = new Path(getPathDfs(tmpDir));
      Path userPath = new Path(getPathDfs(userDir));
      Path userHivePath = new Path(getPathDfs(userHiveDir));
      Path userIdPath = new Path(getPathDfs(userIdDir));
      Path userIdWorkingPath = new Path(getPathDfs(userIdWorkingDir));
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
    debugMessageFooter(LOG, "setUpFileSystem", time);
  }

  protected static String stripLeadingSlashes(String string) {
    int indexAfterLeadingSlash = 0;
    while (indexAfterLeadingSlash < string.length()
        && string.charAt(indexAfterLeadingSlash) == '/')
      ++indexAfterLeadingSlash;
    return indexAfterLeadingSlash == 0 ? string : string.substring(
        indexAfterLeadingSlash, string.length());
  }

  protected static long debugMessageHeader(Logger log, String method) {
    if (log.isDebugEnabled()) {
      log.debug(LOG_PREFIX + " [" + method + "] starting ... ");
    }
    return System.currentTimeMillis();
  }

  protected static void debugMessageFooter(Logger log, String method, long start) {
    long time = System.currentTimeMillis() - start;
    if (log.isDebugEnabled()) {
      log.debug(LOG_PREFIX + " [" + method + "] finished in [" + time + "] ms");
    }
  }

  private static List<File> listFilesLocal(String path, boolean explode,
      String... paths) {
    final File pathFile = new File(ABS_DIR_WORKING + "/" + path);
    if (!pathFile.exists() || !pathFile.isDirectory()) {
      throw new IllegalArgumentException("Could not find directory ["
          + pathFile.getAbsolutePath() + "]");
    }
    List<File> files = new ArrayList<File>();
    for (File pathParentFile : pathFile
        .listFiles((FileFilter) DirectoryFileFilter.DIRECTORY)) {
      if (paths.length == 0 || paths[0].equals(pathParentFile.getName())) {
        for (File pathChildFile : pathParentFile
            .listFiles((FileFilter) DirectoryFileFilter.DIRECTORY)) {
          if (paths.length <= 1 || paths[1].equals(pathChildFile.getName())) {
            for (File pathChildChildFile : pathChildFile
                .listFiles((FileFilter) DirectoryFileFilter.DIRECTORY)) {
              if (paths.length <= 2
                  || paths[2].equals(pathChildChildFile.getName())) {
                for (File pathChildChildFiles : pathChildChildFile.listFiles()) {
                  if (explode && pathChildChildFiles.isDirectory()) {
                    files.addAll(FileUtils.listFiles(pathChildChildFiles,
                        TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE));
                  } else {
                    files.add(pathChildChildFiles);
                  }
                }
              }
            }
          }
        }
      }
    }
    return files;
  }

  private boolean copyFromLocalFile(List<Path> sources, Path destination)
      throws IOException {
    FileSystem fileSystem = getFileSystem();
    for (Path source : sources) {
      File sourceFile = new File(source.toString());
      Path destinationChildPath = new Path(destination, source.getName());
      if (fileSystem.exists(destinationChildPath)) {
        if (sourceFile.isDirectory()
            && fileSystem.isDirectory(destinationChildPath)) {
          List<Path> sourceChildPaths = new ArrayList<Path>();
          for (File sourceChildFile : sourceFile.listFiles()) {
            sourceChildPaths.add(new Path(sourceChildFile.getPath()));
          }
          return copyFromLocalFile(sourceChildPaths, destinationChildPath);
        } else if (sourceFile.isDirectory()
            && fileSystem.isFile(destinationChildPath) || sourceFile.isFile()
            && fileSystem.isDirectory(destinationChildPath)) {
          fileSystem.delete(destinationChildPath, true);
        }
      }
      fileSystem.copyFromLocalFile(source, destination);
    }
    return true;
  }

  protected static String LOG_PREFIX = "Test harness";

  private static Logger LOG = LoggerFactory.getLogger(BaseTest.class);

}
