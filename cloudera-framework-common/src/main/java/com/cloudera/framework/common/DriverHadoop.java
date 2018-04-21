package com.cloudera.framework.common;

import org.apache.hadoop.conf.Configuration;

public abstract class DriverHadoop extends Driver {

  public DriverHadoop() {
    super(Engine.HADOOP);
  }

  public DriverHadoop(Configuration conf) {
    super(conf, Engine.HADOOP);
  }

  public DriverHadoop(boolean enableMetaData) {
    super(Engine.HADOOP, enableMetaData);
  }

  public DriverHadoop(Configuration conf, boolean enableMetaData) {
    super(conf, Engine.HADOOP, enableMetaData);
  }

}
