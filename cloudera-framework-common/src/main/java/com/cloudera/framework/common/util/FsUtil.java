package com.cloudera.framework.common.util;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;

/**
 * Provide DFS utility functions
 */
public class FsUtil {

  /**
   * Provide a file listing of a group of <code>paths</code>, filtering out all
   * system files
   *
   * @param paths to list
   * @return the files
   */
  public static Iterable<File> listFiles(String... paths) {
    List<File> files = new ArrayList<>();
    for (String path : paths) {
      File pathFile = new File(path);
      if (pathFile.exists() && pathFile.isDirectory()) {
        for (File file : FileUtils.listFiles(pathFile, FileFilterUtils.trueFileFilter(), FileFilterUtils.falseFileFilter())) {
          if (!file.getName().startsWith(".")) {
            files.add(file);
          }
        }
      }
    }
    return files;
  }

}
