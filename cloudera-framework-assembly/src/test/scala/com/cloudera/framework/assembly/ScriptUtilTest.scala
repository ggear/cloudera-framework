package com.cloudera.framework.assembly

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.CommonConfigurationKeysPublic
import org.scalatest.{FlatSpec, Matchers}

class ScriptUtilTest extends FlatSpec with Matchers {

  "A Hadoop config" should "should be unadulterated by default" in {
    assert(ScriptUtil.getHadoopDefaultFs.isEmpty)
    assert(ScriptUtil.getHadoopConf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY) ==
      new Configuration().get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY))
  }

  it should "be updated with a default FS if executing as a script" in {
    System.setProperty(ScriptUtil.PropertyHadoopDefaultFs, "hdfs://localhost:89090")
    assert(ScriptUtil.getHadoopDefaultFs.isDefined)
    assert(ScriptUtil.getHadoopConf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY) !=
      new Configuration().get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY))
  }

  "A Spark config" should "should be unadulterated by default" in {
    assert(ScriptUtil.getSparkMaster.isEmpty)
    assert(System.getProperty("spark.master") == null)
  }

  it should "be updated with a default FS if executing as a script" in {
    System.setProperty(ScriptUtil.PropertySparkMaster, "local[1]")
    assert(ScriptUtil.getSparkMaster.isDefined)
    assert(ScriptUtil.getSparkConf != null)
    assert(System.getProperty("spark.master") != null)
    assert(System.getProperty("spark.master") == ScriptUtil.getSparkMaster.get)
  }

}
