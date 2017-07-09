package com.cloudera.framework.example.three

import com.cloudera.framework.common.Driver.RETURN_SUCCESS
import com.cloudera.framework.example.three.Driver.{ModelLabel, RawLabel, TrainLabel}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

object Driver {

  val RawLabel = "raw"
  val TestLabel = "test"
  val TrainLabel = "train"
  val ModelLabel = "model"

}

class Driver extends com.cloudera.framework.common.Driver {

  private val Log = LoggerFactory.getLogger(classOf[Driver])

  var rawPath: Path = _
  var trainPath: Path = _
  var modelPath: Path = _

  def this(configuration: Configuration) {
    this
    super.setConf(configuration)
  }

  def main(arguments: Array[String]): Unit = {
    System.exit(new Driver().runner(arguments))
  }

  override def prepare(arguments: String*): Int = {
    if (arguments == null || arguments.length != parameters().length) throw new Exception("Invalid number of arguments")
    val hdfs = FileSystem.newInstance(getConf)
    val workingPath = hdfs.makeQualified(new Path(arguments(0)))
    if (!hdfs.exists(workingPath)) throw new Exception("Input path [" + workingPath + "] does not exist")
    if (Log.isInfoEnabled) Log.info("Working path [" + workingPath + "] validated")
    rawPath = new Path(workingPath, RawLabel)
    if (!hdfs.exists(rawPath)) hdfs.mkdirs(rawPath)
    trainPath = new Path(workingPath, TrainLabel)
    if (hdfs.exists(trainPath)) hdfs.delete(trainPath, true)
    modelPath = new Path(workingPath, ModelLabel)
    if (hdfs.exists(modelPath)) hdfs.delete(modelPath, true)
    hdfs.mkdirs(modelPath)
    RETURN_SUCCESS
  }

  override def parameters(): Array[String] = {
    Array("working-path")
  }

  override def execute(): Int = {
    Model.build(getConf, rawPath.toString, trainPath.toString, modelPath.toString)
    RETURN_SUCCESS
  }

}
