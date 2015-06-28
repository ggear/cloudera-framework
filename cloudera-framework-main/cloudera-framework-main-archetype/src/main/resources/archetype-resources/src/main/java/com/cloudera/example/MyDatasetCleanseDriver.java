package com.cloudera.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.framework.main.common.Driver;

/**
 * Dataset cleanse driver
 */
public class MyDatasetCleanseDriver extends Driver {

  private static final Logger LOG = LoggerFactory.getLogger(MyDatasetCleanseDriver.class);

  private Path inputPath;
  private Path outputPath;

  public MyDatasetCleanseDriver() {
    super();
  }

  public MyDatasetCleanseDriver(Configuration confguration) {
    super(confguration);
  }

  @Override
  public String description() {
    return "Clense my dataset";
  }

  @Override
  public String[] options() {
    return new String[] {};
  }

  @Override
  public String[] parameters() {
    return new String[] { "input-path", "output-path" };
  }

  @Override
  public int prepare(String... arguments) throws Exception {
    if (arguments == null || arguments.length != 2) {
      throw new Exception("Invalid number of arguments");
    }
    FileSystem hdfs = FileSystem.newInstance(getConf());
    inputPath = new Path(arguments[0]);
    if (!hdfs.exists(inputPath)) {
      throw new Exception("Input path [" + inputPath + "] does not exist");
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("Input path [" + inputPath + "] validated");
    }
    outputPath = new Path(arguments[1]);
    if (hdfs.exists(outputPath)) {
      throw new Exception("Output path [" + outputPath + "] does not exist");
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("Output path [" + outputPath + "] validated");
    }
    return RETURN_SUCCESS;
  }

  @Override
  public int execute() throws Exception {
    return 0;
  }

}
