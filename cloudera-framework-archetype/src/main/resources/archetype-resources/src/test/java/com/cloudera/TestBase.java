package com.cloudera;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;

/**
 * Test base class
 */
public abstract class TestBase implements Constants, TestConstants {

  public static Iterable<File> listFiles(String... paths) {
    List<File> files = new ArrayList<File>();
    for (String path : paths) {
      for (File file : FileUtils.listFiles(new File(path), FileFilterUtils.trueFileFilter(), FileFilterUtils.falseFileFilter())) {
        if (!file.getName().startsWith(".")) {
          files.add(file);
        }
      }
    }
    return files;
  }

}
