package com.cloudera.framework.common;

import org.apache.hadoop.conf.Configuration;

public abstract class DriverHadoop extends Driver {

  public DriverHadoop(Configuration conf) {
    super(conf, Engine.HADOOP);
  }

}
