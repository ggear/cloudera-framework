package com.cloudera.framework.testing.server;

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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.shims.HadoopShims.MiniDFSShim;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DFS {@link TestRule}
 */
public class DfsServer extends CdhServer<DfsServer, DfsServer.Runtime> {

  private static final Logger LOG = LoggerFactory.getLogger(DfsServer.class);

  private static final Path PATH_ROOT = new Path("/");
  private static final FsPermission PERMISSION_ALL = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);

  private static DfsServer instance;

  private MiniDFSShim miniDfs;
  private FileSystem fileSystem;

  private DfsServer(Runtime runtime) {
    super(runtime);
  }

  /**
   * Get instance with default runtime
   *
   * @return
   */
  public static synchronized DfsServer getInstance() {
    return getInstance(instance == null ? Runtime.LOCAL_FS : instance.getRuntime());
  }

  /**
   * Get instance with specific <code>runtime</code>
   *
   * @return
   */
  public static synchronized DfsServer getInstance(Runtime runtime) {
    return instance == null ? instance = new DfsServer(runtime) : instance.assertRuntime(runtime);
  }

  /**
   * Get a local file system path from a local file system <code>path</code>
   *
   * @param path the path relative to the module root, can be with or without '/'
   *             prefix
   * @return the local path
   */
  protected static Path getPathLocal(String path) {
    String pathRelativeToModuleRootSansLeadingSlashes = stripLeadingSlashes(path);
    return new Path(
      pathRelativeToModuleRootSansLeadingSlashes.equals("")
        ? ABS_DIR_WORKING.length() < 2 ? "/" : ABS_DIR_WORKING.substring(0, ABS_DIR_WORKING.length() - 2) : ABS_DIR_WORKING,
      pathRelativeToModuleRootSansLeadingSlashes);
  }

  /**
   * Get a local file system listing of <code>path</code> matching specific
   * dataset, subset and label <code>paths</code>
   *
   * @param path  the path relative to the module root, can be with or without '/'
   *              prefix
   * @param paths optional list of dataset, subset and label paths to include, if
   *              not specified all paths at that level will be included
   * @return the local files
   */
  public static File[] listFilesLocal(String path, String... paths) {
    return listFilesLocal(path, true, paths);
  }

  /**
   * Get a dataset, subset and label keyed map of the local file listing of
   * <code>path</code> matching specific dataset, subset and label
   * <code>paths</code>
   *
   * @param path  the path relative to the module root, can be with or without '/'
   *              prefix
   * @param paths optional list of dataset, subset and label paths to include, if
   *              not specified all paths at that level will be included
   * @return the local files as mapped by dataset, subset and label
   */
  public static Map<String, Map<String, Map<String, List<File>>>> mapFilesLocal(String path, String... paths) {
    Map<String, Map<String, Map<String, List<File>>>> files = new TreeMap<>();
    for (File file : listFilesLocal(path, false, paths)) {
      String pathDataset = file.getParentFile().getParentFile().getParentFile().getName();
      String pathSubset = file.getParentFile().getParentFile().getName();
      String pathLabel = file.getParentFile().getName();
      files.computeIfAbsent(pathDataset, k -> new TreeMap<>());
      files.get(pathDataset).computeIfAbsent(pathSubset, k -> new TreeMap<>());
      files.get(pathDataset).get(pathSubset).computeIfAbsent(pathLabel, k -> new ArrayList<>());
      if (file.isFile()) {
        files.get(pathDataset).get(pathSubset).get(pathLabel).add(file);
      } else {
        files.get(pathDataset).get(pathSubset).get(pathLabel)
          .addAll(FileUtils.listFiles(file, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE));
      }
    }
    return files;
  }

  @SuppressWarnings("ConstantConditions")
  private static File[] listFilesLocal(String path, boolean explode, String... paths) {
    final File pathFile = new File(ABS_DIR_WORKING + "/" + path);
    if (!pathFile.exists() || !pathFile.isDirectory()) {
      throw new IllegalArgumentException("Could not find directory [" + pathFile.getAbsolutePath() + "]");
    }
    List<File> files = new ArrayList<>();
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

  private static String stripLeadingSlashes(String string) {
    int indexAfterLeadingSlash = 0;
    while (indexAfterLeadingSlash < string.length() && string.charAt(indexAfterLeadingSlash) == '/')
      ++indexAfterLeadingSlash;
    return indexAfterLeadingSlash == 0 ? string : string.substring(indexAfterLeadingSlash, string.length());
  }

  /**
   * Get the {@link FileSystem} for clients of this test
   *
   * @return the test file system
   */
  public synchronized FileSystem getFileSystem() {
    return fileSystem;
  }

  /**
   * Get a DFS path from a local file system <code>path</code>
   *
   * @param path the path relative to the DFS root, can be with or without '/'
   *             prefix
   * @return the DFS path
   */
  public Path getPath(String path) {
    switch (getRuntime()) {
      case LOCAL_FS:
        path = (path = stripLeadingSlashes(path)).equals("") ? ABS_DIR_DFS_LOCAL : ABS_DIR_DFS_LOCAL + "/" + path;
        break;
      case CLUSTER_DFS:
        path = "/" + stripLeadingSlashes(path);
        break;
    }
    return new Path(path);
  }

  /**
   * Get a DFS path URI {@link String} from a local file system
   * <code>path</code>
   *
   * @param path the path relative to the DFS root, can be with or without '/'
   *             prefix
   * @return the DFS path URI {@link String}
   */
  public String getPathUri(String path) {
    return getPath(path).makeQualified(getFileSystem().getUri(), PATH_ROOT).toString();
  }

  /**
   * Get a DFS listing of <code>path</code>
   *
   * @param path the path relative to the DFS root, can be with or without '/'
   *             prefix
   * @return the DFS files
   */
  public Path[] listFilesDfs(String path) throws IllegalArgumentException, IOException {
    List<Path> paths = new ArrayList<>();
    try {
      RemoteIterator<LocatedFileStatus> locatedFileStatuses = getFileSystem().listFiles(getPath(path), true);
      while (locatedFileStatuses.hasNext()) {
        paths.add(locatedFileStatuses.next().getPath());
      }
    } catch (FileNotFoundException fileNotFoundException) {
      // ignore
    }
    return paths.toArray(new Path[paths.size()]);
  }

  /**
   * Copy local <code>sourcePath</code> to DFS <code>destinationPath</code>
   *
   * @param sourcePath
   * @param destinationPath
   * @return local files that have been copied
   * @throws IOException
   */
  public File[] copyFromLocalFile(String sourcePath, String destinationPath) throws IOException {
    File file = new File(getPathLocal(sourcePath).toString());
    if (!file.exists()) {
      return new File[0];
    }
    getFileSystem().copyFromLocalFile(getPathLocal(sourcePath), getPath(destinationPath));
    if (file.isFile()) {
      return new File[]{file};
    } else {
      return FileUtils.listFiles(new File(getPathLocal(sourcePath).toString()), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE)
        .toArray(new File[0]);
    }
  }

  /**
   * Copy local <code>sourcePaths</code> matching <code>datasets</code>,
   * <code>subsets</code> and <code>labels</code> paths, to DFS
   * <code>destinationPaths</code>
   *
   * @param sourcePaths      the source paths relative to the module root
   * @param destinationPaths the destination paths relative to the DFS root
   * @param datasets         list of datasets, null will match all dataset paths
   * @param subsets          list of dataset subsets matching child dataset paths, null will
   *                         match all subsets for this indexed dataset
   * @param labels           list of dataset subset labels, null will match all labels for this
   *                         indexed dataset subset
   * @return local files that have been copied
   */
  public File[] copyFromLocalDir(String[] sourcePaths, String[] destinationPaths, String[] datasets, String[][] subsets,
                                 String[][][] labels) throws IllegalArgumentException, IOException {
    List<File> files = new ArrayList<>();
    if (datasets.length != sourcePaths.length || datasets.length != destinationPaths.length || datasets.length != subsets.length
      || datasets.length != labels.length) {
      throw new IllegalArgumentException("Number of datasets exceeds number of source paths, destination paths, subsets and or labels");
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
            files.addAll(Arrays.asList(copyFromLocalDir(sourcePaths[i], destinationPaths[i], datasets[i], subsets[i][j])));
          } else {
            files.addAll(Arrays.asList(copyFromLocalDir(sourcePaths[i], destinationPaths[i], datasets[i], subsets[i][j], labels[i][j][k])));
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
   * @param sourcePath      the source path relative to the module root
   * @param destinationPath the destination path relative to the DFS root
   * @param sourcePaths     optional list of dataset, subset and label paths to include, if
   *                        not specified all paths at that level will be included
   * @return local files that have been copied
   */
  public File[] copyFromLocalDir(String sourcePath, String destinationPath, String... sourcePaths)
    throws IllegalArgumentException, IOException {
    long time = log(LOG, "copy", true);
    List<File> files = new ArrayList<>();
    StringBuilder filesString = new StringBuilder();
    String sourcePathGlob = ((sourcePaths.length == 0 ? "*" : sourcePaths[0]) + "/" + (sourcePaths.length <= 1 ? "*" : sourcePaths[1]) + "/"
      + (sourcePaths.length <= 2 ? "*" : sourcePaths[2])).replace(ABS_DIR_WORKING, ".");
    getFileSystem().mkdirs(getPath(destinationPath));
    for (File file : listFilesLocal(sourcePath, false, sourcePaths)) {
      copyFromLocalFile(Collections.singletonList(new Path(file.getPath())), getPath(destinationPath));
      if (file.isFile()) {
        files.add(file);
      } else {
        files.addAll(FileUtils.listFiles(file, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE));
      }
      filesString.append("\n").append("/").append(file.getParentFile().getParentFile().getParentFile().getName()).append("/").
        append(file.getParentFile().getParentFile().getName()).append("/").append(file.getParentFile().getName()).append("/").
        append(file.getName()).append(file.isDirectory() ? "/" : "").append(" -> ").append(destinationPath).append("/").append(file.getName());
    }
    log(LOG, "copy", "cp -rvf /" + sourcePathGlob + "/* " + destinationPath + (filesString.length() > 0 ? filesString.toString() : ":\n  "),
      true);
    if (files.isEmpty()) {
      throw new IllegalArgumentException("Could not find files with path [" + sourcePathGlob + "]");
    }
    log(LOG, "copy", time, true);
    return files.toArray(new File[files.size()]);
  }

  @Override
  public int getIndex() {
    return 10;
  }

  @Override
  public synchronized void start() throws IOException {
    long time = log(LOG, "start");
    switch (getRuntime()) {
      case LOCAL_FS:
        fileSystem = FileSystem.getLocal(getConf());
        break;
      case CLUSTER_DFS:
        fileSystem = (miniDfs = ShimLoader.getHadoopShims().getMiniDfs(getConf(), 1, true, null)).getFileSystem();
        setConf(fileSystem.getConf());
        break;
      default:
        throw new IllegalArgumentException("Unsupported [" + getClass().getSimpleName() + "] runtime [" + getRuntime() + "]");
    }
    log(LOG, "start", time);
  }

  @Override
  public synchronized void clean() throws IOException {
    long time = log(LOG, "clean");
    if (!fileSystem.exists(getPath("/"))) {
      FileSystem.mkdirs(getFileSystem(), getPath("/"), PERMISSION_ALL);
    }
    for (FileStatus fileStatus : fileSystem.listStatus(getPath("/"))) {
      fileSystem.delete(fileStatus.getPath(), true);
    }
    FileSystem.mkdirs(getFileSystem(), getPath("/tmp"), PERMISSION_ALL);
    FileSystem.mkdirs(getFileSystem(), getPath("/usr"), PERMISSION_ALL);
    FileSystem.mkdirs(getFileSystem(), getPath("/usr/" + System.getProperty("user.name")), PERMISSION_ALL);
    log(LOG, "clean", time);
  }

  @Override
  public synchronized void state() throws IllegalArgumentException, IOException {
    long time = log(LOG, "state", true);
    Path[] files = listFilesDfs("/");
    Arrays.sort(files);
    StringBuilder filesString = new StringBuilder();
    for (Path file : files) {
      if (!file.toString().contains(DIR_RUNTIME_MR)) {
        filesString.append("\n")
          .append(Path.getPathWithoutSchemeAndAuthority(file).toString().replace(getPathLocal(REL_DIR_DFS_LOCAL).toUri().toString(), ""));
      }
    }
    log(LOG, "state", "find / -type f:" + (filesString.length() > 0 ? filesString.toString() : "\n"), true);
    log(LOG, "state", time, true);
  }

  @Override
  public synchronized void stop() throws IOException {
    long time = log(LOG, "stop");
    if (fileSystem != null) {
      fileSystem.close();
      fileSystem = null;
    }
    if (miniDfs != null) {
      miniDfs.shutdown();
      miniDfs = null;
    }
    log(LOG, "stop", time);
  }

  @SuppressWarnings("ConstantConditions")
  private boolean copyFromLocalFile(List<Path> sources, Path destination) throws IOException {
    FileSystem fileSystem = getFileSystem();
    for (Path source : sources) {
      File sourceFile = new File(source.toString());
      Path destinationChildPath = new Path(destination, source.getName());
      if (fileSystem.exists(destinationChildPath)) {
        if (sourceFile.isDirectory() && fileSystem.isDirectory(destinationChildPath)) {
          List<Path> sourceChildPaths = new ArrayList<>();
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

  public enum Runtime {
    LOCAL_FS, // Local file-system DFS facade, inline-thread, light-weight
    CLUSTER_DFS // Mini DFS cluster, multi-threaded, heavy-weight
  }

}
