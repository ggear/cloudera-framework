package com.cloudera.framework.assembly

import java.io.PrintStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{CommonConfigurationKeysPublic, FileSystem, Path}
import org.apache.spark.SparkConf

import scala.io.Source

object ScriptUtil {

  def getHadoopConf: Configuration = {
    val conf = new Configuration
    if (getHadoopDefaultFs.isDefined) conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, getHadoopDefaultFs.get)
    conf
  }

  def getSparkConf: SparkConf = {
    if (getSparkMaster.isDefined) {
      System.setProperty("spark.master", getSparkMaster.get)
      System.setProperty("spark.app.name", "spark-script-test")
      System.setProperty("spark.sql.warehouse.dir", getHadoopDefaultFs.get + "/usr/spark/warehouse")
    }
    new SparkConf
  }

  def getHadoopDefaultFs: Option[String] = {
    var property = None: Option[String]
    if (System.getProperty(PropertyHadoopDefaultFs) != null) property = Some(System.getProperty(PropertyHadoopDefaultFs))
    else if (System.getenv(PropertyHadoopDefaultFs) != null) property = Some(System.getenv(PropertyHadoopDefaultFs))
    property
  }

  def PropertyHadoopDefaultFs = "CF_HADOOP_DEFAULT_FS"

  def getSparkMaster: Option[String] = {
    var property = None: Option[String]
    if (System.getProperty(PropertySparkMaster) != null) property = Some(System.getProperty(PropertySparkMaster))
    else if (System.getenv(PropertySparkMaster) != null) property = Some(System.getenv(PropertySparkMaster))
    property
  }

  def PropertySparkMaster = "CF_SPARK_MASTER"

  def copyFromUrl(hdfs: FileSystem, to: Path, from: String): Unit = {
    new PrintStream(hdfs.create(to)) {
      print(Source.fromURL(from).mkString)
      close()
    }
  }

}
