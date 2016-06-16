package com.cloudera.framework.common.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Provide DFS utility functions
 */
public class DfsUtil {

  private static final String FILE_METADATA_PREFIX = "_";

  /**
   * Determine whether an action can be performed
   *
   * @param hdfs
   *          the {@link FileSystem file system}
   * @param user
   *          the user to test
   * @param groups
   *          the groups to test
   * @param path
   *          the {@link Path} to test
   * @param action
   *          the action to test
   * @return <code>true</code> if action is allowed, <code>false</code>
   *         otherwise
   */
  public static boolean canDoAction(FileSystem hdfs, String user, String[] groups, Path path, FsAction action) throws IOException {
    FileStatus status = hdfs.getFileStatus(path);
    FsPermission permission = status.getPermission();
    if (permission.getOtherAction().implies(action)) {
      return true;
    }
    for (String group : groups) {
      if (group.equals(status.getGroup()) && permission.getGroupAction().implies(action)) {
        return true;
      }
    }
    if (user.equals(status.getOwner()) && permission.getUserAction().implies(action)) {
      return true;
    }
    return false;
  }

  /**
   * List files.
   *
   * @param hdfs
   *          the {@link FileSystem file system}
   * @param path
   *          the root {@link Path path}
   * @param recurse
   *          whether to recurse from <code>path</code>
   * @param filterMetaData
   *          if <code>true</code> filter out files named with suffix
   *          {@link #FILE_METADATA_PREFIX}
   * @return the {@link Set set} of file {@link Path paths}
   */
  public static Set<Path> listFiles(FileSystem hdfs, Path path, boolean recurse, boolean filterMetaData)
      throws FileNotFoundException, IOException {
    Set<Path> files = new HashSet<>();
    try {
      RemoteIterator<LocatedFileStatus> filesIterator = hdfs.listFiles(path, recurse);
      while (filesIterator.hasNext()) {
        Path file = filesIterator.next().getPath();
        if (!filterMetaData || !file.getName().startsWith(FILE_METADATA_PREFIX)) {
          files.add(file);
        }
      }
    } catch (FileNotFoundException exception) {
      // ignore
    }
    return files;
  }

  /**
   * List directories.
   *
   * @param hdfs
   *          the {@link FileSystem file system}
   * @param path
   *          the root {@link Path path}
   * @param recurse
   *          whether to recurse from <code>path</code>
   * @param filterMetaData
   *          if <code>true</code> filter out directories either named or with a
   *          containing file named with suffix {@link #FILE_METADATA_PREFIX}
   * @return the {@link Set set} of directory {@link Path paths}
   */
  public static Set<Path> listDirs(FileSystem hdfs, Path path, boolean recurse, boolean filterMetaData)
      throws FileNotFoundException, IOException {
    Map<Path, Boolean> dirs = new HashMap<>();
    try {
      RemoteIterator<LocatedFileStatus> filesIterator = hdfs.listFiles(path, recurse);
      while (filesIterator.hasNext()) {
        Path file = filesIterator.next().getPath();
        Path dir = file.getParent();
        dirs.put(dir, (dirs.containsKey(dir) ? dirs.get(dir) : false) || file.getName().startsWith(FILE_METADATA_PREFIX));
      }
    } catch (FileNotFoundException exception) {
      // ignore
    }
    Set<Path> dirsFiltered = dirs.keySet();
    if (filterMetaData) {
      dirsFiltered = new HashSet<>();
      for (Path dir : dirs.keySet()) {
        if (!dirs.get(dir) && !dir.getName().startsWith(FILE_METADATA_PREFIX)) {
          dirsFiltered.add(dir);
        }
      }
    }
    return dirsFiltered;
  }

}
