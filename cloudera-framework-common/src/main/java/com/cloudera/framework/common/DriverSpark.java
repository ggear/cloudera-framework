package com.cloudera.framework.common;

import org.apache.hadoop.conf.Configuration;

public abstract class DriverSpark extends Driver {

  public DriverSpark() {
    super(Engine.SPARK);
  }

  public DriverSpark(Configuration conf) {
    super(conf, Engine.SPARK);
  }

  public DriverSpark(boolean enableMetaData) {
    super(Engine.SPARK, enableMetaData);
  }

  public DriverSpark(Configuration conf, boolean enableMetaData) {
    super(conf, Engine.SPARK, enableMetaData);
  }

}
