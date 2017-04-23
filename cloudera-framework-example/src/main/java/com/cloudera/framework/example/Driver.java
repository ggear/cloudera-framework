package com.cloudera.framework.example;

import java.util.Set;

import com.cloudera.framework.common.util.DfsUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Driver super class
 */
public abstract class Driver extends com.cloudera.framework.common.Driver {

  private static final Logger LOG = LoggerFactory.getLogger(Driver.class);

  @SuppressWarnings("FieldCanBeLocal")
  protected Path inputPath;
  protected Path outputPath;
  protected Set<Path> inputPaths;

  @SuppressWarnings("FieldCanBeLocal")
  protected FileSystem hdfs;

  public Driver() {
    super();
  }

  public Driver(Configuration configuration) {
    super(configuration);
  }

  @Override
  public int prepare(String... arguments) throws Exception {
    if (arguments == null || arguments.length != 2) {
      throw new Exception("Invalid number of arguments");
    }
    hdfs = FileSystem.newInstance(getConf());
    inputPath = hdfs.makeQualified(new Path(arguments[0]));
    if (LOG.isInfoEnabled()) {
      LOG.info("Input path [" + inputPath + "] validated");
    }
    inputPaths = DfsUtil.listDirs(hdfs, inputPath, true, true);
    outputPath = hdfs.makeQualified(new Path(arguments[1]));
    hdfs.mkdirs(outputPath.getParent());
    if (LOG.isInfoEnabled()) {
      LOG.info("Output path [" + outputPath + "] validated");
    }
    return RETURN_SUCCESS;
  }

}
