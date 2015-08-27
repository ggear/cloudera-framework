package com.cloudera.framework.main.test;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import parquet.Log;
import parquet.hadoop.ParquetOutputFormat;
import uk.org.lidalia.sysoutslf4j.context.SysOutOverSLF4J;

/**
 * Base class for all unit tests, not intended for direct extension, the
 * concrete classes in this package are used for this purpose
 */
public abstract class BaseTest {

  // Directories
  public static final String DIR_TARGET = "target";
  public static final String DIR_DATASET = "data";
  public static final String DIR_DATA = "test-data";
  public static final String DIR_CLASSES = "test-classes";
  public static final String DIR_FS_TMP = "test-fs-tmp";
  public static final String DIR_FS_LOCAL = "test-fs-local";
  public static final String DIR_DFS_LOCAL = "test-hdfs-local";
  public static final String DIR_DFS_MINICLUSTER = "test-hdfs-minicluster";
  public static final String DIR_MINICLUSTER_PREFIX = "MiniMRCluster_";

  // Relative directories
  public static final String REL_DIR_DATA = DIR_TARGET + "/" + DIR_DATA;
  public static final String REL_DIR_CLASSES = DIR_TARGET + "/" + DIR_CLASSES;
  public static final String REL_DIR_DATASET = REL_DIR_CLASSES + "/" + DIR_DATASET;
  public static final String REL_DIR_FS_TMP = DIR_TARGET + "/" + DIR_FS_TMP;
  public static final String REL_DIR_FS_LOCAL = DIR_TARGET + "/" + DIR_FS_LOCAL;
  public static final String REL_DIR_DFS_LOCAL = DIR_TARGET + "/" + DIR_DFS_LOCAL;
  public static final String REL_DIR_DFS_MINICLUSTER = DIR_TARGET + "/" + DIR_DFS_MINICLUSTER;

  // Absolute directories
  public static final String ABS_DIR_WORKING = new File(".").getAbsolutePath();
  public static final String ABS_DIR_TARGET = ABS_DIR_WORKING + "/" + DIR_TARGET;
  public static final String ABS_DIR_DATA = ABS_DIR_TARGET + "/" + DIR_DATA;
  public static final String ABS_DIR_FS_TMP = ABS_DIR_TARGET + "/" + DIR_FS_TMP;
  public static final String ABS_DIR_DFS_LOCAL = ABS_DIR_TARGET + "/" + DIR_DFS_LOCAL;
  public static final String ABS_DIR_DFS_MINICLUSTER = ABS_DIR_TARGET + "/" + DIR_DFS_MINICLUSTER;

  protected static String LOG_PREFIX = "Test harness";

  private static Logger LOG = LoggerFactory.getLogger(BaseTest.class);

  /**
   * Default parameters for a {@link Parameterized} runner, all datasets,
   * subsets, labels and metadata suitable for loading into an extending class
   * via
   * {@link #BaseTest(String[], String[], String[], String[][], String[][][], Map[])
   * implementation, and later invoked via {@link #setUpDatasets()},
   * {@link #assertCounterEquals(Map, Map)} or manually
   *
   * @return
   */
  public static Iterable<Object[]> parameters() {
    return Arrays.asList(new Object[][] {
        //
        {
            //
            new String[] { REL_DIR_DATASET, }, //
            new String[] { DIR_DATASET, }, //
            new String[] { null, }, //
            new String[][] { { null, }, }, //
            new String[][][] { { { null }, }, }, //
            new Map[] { Collections.EMPTY_MAP, }, //
        }, //
    });
  }

  public String[] sources;
  public String[] destinations;
  public String[] datasets;
  public String[][] subsets;
  public String[][][] labels;
  @SuppressWarnings("rawtypes")
  public Map[] metadata;

  public BaseTest() {
  }

  public BaseTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets, String[][][] labels,
      @SuppressWarnings("rawtypes") Map[] metadata) {
    this.sources = sources;
    this.destinations = destinations;
    this.datasets = datasets;
    this.subsets = subsets;
    this.labels = labels;
    this.metadata = metadata;
  }

  /**
   * Get the {@link Configuration} for clients of this test
   *
   * @return the test conf
   */
  public abstract Configuration getConf();

  /**
   * Get the {@link FileSystem} for clients of this test
   *
   * @return the test file system
   */
  public abstract FileSystem getFileSystem();

  /**
   * Get a local file system path from a local file system <code>path</code>
   *
   * @param path
   *          the path relative to the module root, can be with or without '/'
   *          prefix
   * @return the local path
   */
  public static String getPathLocal(String path) {
    String pathRelativeToModuleRootSansLeadingSlashes = stripLeadingSlashes(path);
    return pathRelativeToModuleRootSansLeadingSlashes.equals("")
        ? ABS_DIR_WORKING.length() < 2 ? "/" : ABS_DIR_WORKING.substring(0, ABS_DIR_WORKING.length() - 2)
        : new Path(ABS_DIR_WORKING, pathRelativeToModuleRootSansLeadingSlashes).toUri().toString();
  }

  /**
   * Get a DFS path from a local file system <code>path</code>
   *
   * @param path
   *          the path relative to the DFS root, can be with or without '/'
   *          prefix
   * @return the DFS path
   */
  public String getPathDfs(String path) {
    return path;
  }

  /**
   * Get a local file system listing of <code>path</code> matching specific
   * dataset, subset and label <code>paths</code>
   *
   * @param path
   *          the path relative to the module root, can be with or without '/'
   *          prefix
   * @param paths
   *          optional list of dataset, subset and label paths to include, if
   *          not specified all paths at that level will be included
   * @return the local files
   */
  public static File[] listFilesLocal(String path, String... paths) {
    return listFilesLocal(path, true, paths);
  }

  /**
   * Get a DFS listing of <code>path</code>
   *
   * @param path
   *          the path relative to the DFS root, can be with or without '/'
   *          prefix
   * @return the DFS files
   */
  public Path[] listFilesDfs(String path) throws IllegalArgumentException, IOException {
    List<Path> paths = new ArrayList<Path>();
    try {
      RemoteIterator<LocatedFileStatus> locatedFileStatuses = getFileSystem().listFiles(new Path(getPathDfs(path)),
          true);
      while (locatedFileStatuses.hasNext()) {
        paths.add(locatedFileStatuses.next().getPath());
      }
    } catch (FileNotFoundException fileNotFoundException) {
      // ignore
    }
    return paths.toArray(new Path[paths.size()]);
  }

  /**
   * Get a dataset, subset and label keyed map of the local file listing of
   * <code>path</code> matching specific dataset, subset and label
   * <code>paths</code>
   *
   * @param path
   *          the path relative to the module root, can be with or without '/'
   *          prefix
   * @param paths
   *          optional list of dataset, subset and label paths to include, if
   *          not specified all paths at that level will be included
   * @return the local files as mapped by dataset, subset and label
   */
  public static Map<String, Map<String, Map<String, List<File>>>> mapFilesLocal(String path, String... paths) {
    Map<String, Map<String, Map<String, List<File>>>> files = new TreeMap<String, Map<String, Map<String, List<File>>>>();
    for (File file : listFilesLocal(path, false, paths)) {
      String pathDataset = file.getParentFile().getParentFile().getParentFile().getName();
      String pathSubset = file.getParentFile().getParentFile().getName();
      String pathLabel = file.getParentFile().getName();
      if (files.get(pathDataset) == null) {
        files.put(pathDataset, new TreeMap<String, Map<String, List<File>>>());
      }
      if (files.get(pathDataset).get(pathSubset) == null) {
        files.get(pathDataset).put(pathSubset, new TreeMap<String, List<File>>());
      }
      if (files.get(pathDataset).get(pathSubset).get(pathLabel) == null) {
        files.get(pathDataset).get(pathSubset).put(pathLabel, new ArrayList<File>());
      }
      if (file.isFile()) {
        files.get(pathDataset).get(pathSubset).get(pathLabel).add(file);
      } else {
        files.get(pathDataset).get(pathSubset).get(pathLabel)
            .addAll(FileUtils.listFiles(file, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE));
      }
    }
    return files;
  }

  /**
   * Copy local <code>sourcePaths</code> matching <code>datasets</code>,
   * <code>subsets</code> and <code>labels</code> paths, to DFS
   * <code>destinationPaths</code>
   *
   * @param sourcePaths
   *          the source paths relative to the module root
   * @param destinationPaths
   *          the destination paths relative to the DFS root
   * @param datasets
   *          list of datasets, null will match all dataset paths
   * @param subsets
   *          list of dataset subsets matching child dataset paths, null will
   *          match all subsets for this indexed dataset
   * @param labels
   *          list of dataset subset labels, null will match all labels for this
   *          indexed dataset subset
   * @return local files that have been copied
   */
  public File[] copyFromLocalDir(String[] sourcePaths, String[] destinationPaths, String[] datasets, String[][] subsets,
      String[][][] labels) throws IllegalArgumentException, IOException {
    List<File> files = new ArrayList<File>();
    if (datasets.length != sourcePaths.length || datasets.length != destinationPaths.length
        || datasets.length != subsets.length || datasets.length != labels.length) {
      throw new IllegalArgumentException(
          "Number of datasets exceeds number of source paths, destination paths, subsets and or labels");
    }
    for (int i = 0; i < datasets.length; i++) {
      if (subsets[i].length != labels[i].length) {
        throw new IllegalArgumentException("Number of subsets exceeds number of labels");
      }
      for (int j = 0; j < subsets[i].length; j++) {
        for (int k = 0; k < labels[i][j].length; k++) {
          if (datasets[i] == null) {
            files.addAll(Arrays.asList(copyFromLocalDir(sourcePaths[i], destinationPaths[i])));
          } else if (subsets[i][j] == null) {
            files.addAll(Arrays.asList(copyFromLocalDir(sourcePaths[i], destinationPaths[i], datasets[i])));
          } else if (labels[i][j][k] == null) {
            files.addAll(
                Arrays.asList(copyFromLocalDir(sourcePaths[i], destinationPaths[i], datasets[i], subsets[i][j])));
          } else {
            files.addAll(Arrays.asList(
                copyFromLocalDir(sourcePaths[i], destinationPaths[i], datasets[i], subsets[i][j], labels[i][j][k])));
          }
        }
      }
    }
    return files.toArray(new File[files.size()]);
  }

  /**
   * Copy a local <code>sourcePath</code> matching <code>dataset</code>,
   * <code>subset</code> and <code>label</code> paths, to DFS
   * <code>destinationPath</code>
   *
   * @param sourcePath
   *          the source path relative to the module root
   * @param destinationPath
   *          the destination path relative to the DFS root
   * @param paths
   *          optional list of dataset, subset and label paths to include, if
   *          not specified all paths at that level will be included
   * @return local files that have been copied
   */
  public File[] copyFromLocalDir(String sourcePath, String destinationPath, String... sourcePaths)
      throws IllegalArgumentException, IOException {
    long time = debugMessageHeader(LOG, "copyFromLocalDir");
    List<File> files = new ArrayList<File>();
    StringBuilder filesString = new StringBuilder();
    String sourcePathGlob = ((sourcePaths.length == 0 ? "*" : sourcePaths[0]) + "/"
        + (sourcePaths.length <= 1 ? "*" : sourcePaths[1]) + "/" + (sourcePaths.length <= 2 ? "*" : sourcePaths[2]))
            .replace(ABS_DIR_WORKING, ".");
    getFileSystem().mkdirs(new Path(getPathDfs(destinationPath)));
    for (File file : listFilesLocal(sourcePath, false, sourcePaths)) {
      copyFromLocalFile(Arrays.asList(new Path(file.getPath())), new Path(getPathDfs(destinationPath)));
      if (file.isFile()) {
        files.add(file);
      } else {
        files.addAll(FileUtils.listFiles(file, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE));
      }
      if (LOG.isDebugEnabled()) {
        filesString.append("\n").append("/")
            .append(file.getParentFile().getParentFile().getParentFile().getName() + "/"
                + file.getParentFile().getParentFile().getName() + "/" + file.getParentFile().getName() + "/"
                + file.getName() + (file.isDirectory() ? "/" : ""))
            .append(" -> ").append(destinationPath).append("/").append(file.getName());
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(LOG_PREFIX + " [copyFromLocalDir] cp -rvf /" + sourcePathGlob + "/* " + destinationPath + ":"
          + (filesString.length() > 0 ? filesString.toString() : "\n"));
    }
    if (files.isEmpty()) {
      throw new IllegalArgumentException("Could not find files with path [" + sourcePathGlob + "]");
    }
    debugMessageFooter(LOG, "copyFromLocalDir", time);
    return files.toArray(new File[files.size()]);
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
    System.setProperty("hadoop.tmp.dir", ABS_DIR_FS_TMP);
    System.setProperty("dir.working.target.derby", ABS_DIR_WORKING + "/target/derby");
    System.setProperty("dir.working.target.derby.db", System.getProperty("dir.working.target.derby") + "/db");
    System.setProperty("derby.stream.error.file", System.getProperty("dir.working.target.derby") + "/derby.log");
    for (File file : new File(ABS_DIR_TARGET).listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.isDirectory() && pathname.getName().startsWith(DIR_MINICLUSTER_PREFIX);
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
          if (!fileStatus.getPath().getName().equals(userIdWorkingPath.getName())) {
            fileSystem.delete(fileStatus.getPath(), true);
          }
        }
      }
      if (fileSystem.exists(userIdWorkingPath)) {
        for (FileStatus fileStatus : fileSystem.listStatus(userIdWorkingPath)) {
          if (!fileStatus.getPath().getName().startsWith(userIdWorkingDirPrefix)) {
            fileSystem.delete(fileStatus.getPath(), true);
          }
        }
      }
      fileSystem.mkdirs(tmpPath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
      fileSystem.mkdirs(userHivePath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
      fileSystem.mkdirs(userIdPath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    }
    debugMessageFooter(LOG, "setUpFileSystem", time);
  }

  @After
  public void tearDownFileSystem() throws Exception {
    long time = debugMessageHeader(LOG, "tearDownFileSystem");
    Path[] files = listFilesDfs("/");
    Arrays.sort(files);
    StringBuilder filesString = new StringBuilder();
    for (Path file : files) {
      if (!file.toString().contains(DIR_MINICLUSTER_PREFIX)) {
        filesString.append("\n").append(
            Path.getPathWithoutSchemeAndAuthority(file).toString().replace(getPathLocal(REL_DIR_DFS_LOCAL), ""));
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(LOG_PREFIX + " [tearDownFileSystem] find / -type f:"
          + (filesString.length() > 0 ? filesString.toString() : "\n"));
    }
    debugMessageFooter(LOG, "tearDownFileSystem", time);
  }

  @Before
  public void setUpDatasets() throws IllegalArgumentException, IOException {
    if (sources != null && destinations != null) {
      copyFromLocalDir(sources, destinations, datasets, subsets, labels);
    }
  }

  /**
   * Assert <code>actual</code> equals <code>expected</code>
   *
   * @param expected
   *          <code>Map<String, Map<Enum<?>, Long>></code>
   * @param actual
   *          <code>Map<String, Map<Enum<?>, Long>></code>
   */
  public static void assertCounterEquals(@SuppressWarnings("rawtypes") Map expected,
      Map<String, Map<Enum<?>, Long>> actual) {
    assertCounterEqualsLessThanGreaterThan(expected, actual, true, false, false, false);
  }

  /**
   * Assert <code>actual</code> less than <code>expected</code>
   *
   * @param expected
   *          <code>Map<String, Map<Enum<?>, Long>></code>
   * @param actual
   *          <code>Map<String, Map<Enum<?>, Long>></code>
   */
  public static void assertCounterLessThan(@SuppressWarnings("rawtypes") Map expected,
      Map<String, Map<Enum<?>, Long>> actual) {
    assertCounterEqualsLessThanGreaterThan(expected, actual, false, true, false, false);
  }

  /**
   * Assert <code>actual</code> greater than <code>expected</code>
   *
   * @param expected
   *          <code>Map<String, Map<Enum<?>, Long>></code>
   * @param actual
   *          <code>Map<String, Map<Enum<?>, Long>></code>
   */
  public static void assertCounterGreaterThan(@SuppressWarnings("rawtypes") Map expected,
      Map<String, Map<Enum<?>, Long>> actual) {
    assertCounterEqualsLessThanGreaterThan(expected, actual, false, false, true, false);
  }

  /**
   * Assert <code>actual</code> equals <code>expectedEquals</code>, less then
   * <code>expectedLessThan</code> and greater than
   * <code>expectedGreaterThan</code>
   *
   * @param expectedEquals
   *          <code>Map<String, Map<Enum<?>, Long>></code>, may be empty
   * @param expectedLessThan
   *          <code>Map<String, Map<Enum<?>, Long>></code>, may be empty
   * @param expectedGreaterThan
   *          <code>Map<String, Map<Enum<?>, Long>></code>, may be empty
   * @param actual
   *          <code>Map<String, Map<Enum<?>, Long>></code>
   */
  public static void assertCounterEqualsLessThanGreaterThan(@SuppressWarnings("rawtypes") Map expectedEquals,
      @SuppressWarnings("rawtypes") Map expectedLessThan, @SuppressWarnings("rawtypes") Map expectedGreaterThan,
      Map<String, Map<Enum<?>, Long>> actual) {
    assertCounterEqualsLessThanGreaterThan(expectedEquals, actual, true, false, false, true);
    assertCounterEqualsLessThanGreaterThan(expectedLessThan, actual, false, true, false, true);
    assertCounterEqualsLessThanGreaterThan(expectedGreaterThan, actual, false, false, true, true);
  }

  @SuppressWarnings("unchecked")
  private static void assertCounterEqualsLessThanGreaterThan(@SuppressWarnings("rawtypes") Map expected,
      Map<String, Map<Enum<?>, Long>> actual, boolean assertEquals, boolean assertLessThan, boolean assertGreaterThan,
      boolean isBatch) {
    if (!isBatch) {
      Assert.assertEquals("Expected and actual array sizes differ", expected.size(), actual.size());
    }
    for (String group : actual.keySet()) {
      if (!isBatch || expected.containsKey(group)) {
        Assert.assertTrue("Expected does not contain group [" + group + "]", expected.containsKey(group));
        if (!isBatch) {
          Assert.assertEquals("Expected and actual group [" + group + "] map sizes differ",
              ((Map<Enum<?>, Long>) expected.get(group)).size(), actual.get(group).size());
        }
        for (Enum<?> counter : actual.get(group).keySet()) {
          if (!isBatch || ((Map<Enum<?>, Long>) expected.get(group)).containsKey(counter)) {
            Assert.assertTrue("Expected group [" + group + "] does not contain counter [" + counter + "]",
                ((Map<Enum<?>, Long>) expected.get(group)).containsKey(counter));
            Long expectedLong = ((Map<Enum<?>, Long>) expected.get(group)).get(counter);
            Long actualLong = actual.get(group).get(counter);
            if (assertEquals) {
              Assert.assertTrue("Expected [" + expectedLong + "] to be equal to actual [" + actualLong + "]",
                  expectedLong.equals(actualLong));
            }
            if (assertLessThan) {
              Assert.assertTrue("Expected [" + expectedLong + "] to be greater than actual [" + actualLong + "]",
                  expectedLong > actualLong);
            }
            if (assertGreaterThan) {
              Assert.assertTrue("Expected [" + expectedLong + "] to be less than actual [" + actualLong + "]",
                  expectedLong < actualLong);
            }
          }
        }
      }
    }
  }

  protected static String stripLeadingSlashes(String string) {
    int indexAfterLeadingSlash = 0;
    while (indexAfterLeadingSlash < string.length() && string.charAt(indexAfterLeadingSlash) == '/')
      ++indexAfterLeadingSlash;
    return indexAfterLeadingSlash == 0 ? string : string.substring(indexAfterLeadingSlash, string.length());
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

  private static File[] listFilesLocal(String path, boolean explode, String... paths) {
    final File pathFile = new File(ABS_DIR_WORKING + "/" + path);
    if (!pathFile.exists() || !pathFile.isDirectory()) {
      throw new IllegalArgumentException("Could not find directory [" + pathFile.getAbsolutePath() + "]");
    }
    List<File> files = new ArrayList<File>();
    for (File pathDatasetFile : pathFile.listFiles((FileFilter) DirectoryFileFilter.DIRECTORY)) {
      if (paths.length == 0 || paths[0].equals(pathDatasetFile.getName())) {
        for (File pathSubsetFile : pathDatasetFile.listFiles((FileFilter) DirectoryFileFilter.DIRECTORY)) {
          if (paths.length <= 1 || paths[1].equals(pathSubsetFile.getName())) {
            for (File pathLabelFile : pathSubsetFile.listFiles((FileFilter) DirectoryFileFilter.DIRECTORY)) {
              if (paths.length <= 2 || paths[2].equals(pathLabelFile.getName())) {
                for (File pathLabelFiles : pathLabelFile.listFiles()) {
                  if (explode && pathLabelFiles.isDirectory()) {
                    files.addAll(FileUtils.listFiles(pathLabelFiles, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE));
                  } else {
                    files.add(pathLabelFiles);
                  }
                }
              }
            }
          }
        }
      }
    }
    return files.toArray(new File[files.size()]);
  }

  private boolean copyFromLocalFile(List<Path> sources, Path destination) throws IOException {
    FileSystem fileSystem = getFileSystem();
    for (Path source : sources) {
      File sourceFile = new File(source.toString());
      Path destinationChildPath = new Path(destination, source.getName());
      if (fileSystem.exists(destinationChildPath)) {
        if (sourceFile.isDirectory() && fileSystem.isDirectory(destinationChildPath)) {
          List<Path> sourceChildPaths = new ArrayList<Path>();
          for (File sourceChildFile : sourceFile.listFiles()) {
            sourceChildPaths.add(new Path(sourceChildFile.getPath()));
          }
          return copyFromLocalFile(sourceChildPaths, destinationChildPath);
        } else if (sourceFile.isDirectory() && fileSystem.isFile(destinationChildPath)
            || sourceFile.isFile() && fileSystem.isDirectory(destinationChildPath)) {
          fileSystem.delete(destinationChildPath, true);
        }
      }
      fileSystem.copyFromLocalFile(source, destination);
    }
    return true;
  }

}
