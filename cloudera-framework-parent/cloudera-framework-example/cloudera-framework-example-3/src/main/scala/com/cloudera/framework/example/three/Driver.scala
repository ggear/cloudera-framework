package com.cloudera.framework.example.three

import java.util.Properties

import com.cloudera.framework.common.Driver.Engine.SPARK
import com.cloudera.framework.common.Driver.{Engine, SUCCESS, FAILURE_ARGUMENTS}
import com.cloudera.framework.example.three.Driver.{ModelDir, TestDir, TrainDir}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

import scala.io.Source

object Driver {

  val TestDir = "test"
  val TrainDir = "train"
  val ModelDir = "model"

  val ModelFile = "occupancy.pmml"
}

class Driver extends com.cloudera.framework.common.Driver {

  val Log = LoggerFactory.getLogger(classOf[Driver])

  val properties = new Properties()

  var testPath: Path = _
  var trainPath: Path = _
  var modelPath: Path = _

  setEngine(Engine.SPARK)

  def this(configuration: Configuration) {
    this
    setConf(configuration)
  }

  override def prepare(arguments: String*): Int = {
    if (arguments == null || arguments.length != parameters().length) return FAILURE_ARGUMENTS
    properties.load(Source.fromURL(getClass.getResource("/application.properties")).bufferedReader())
    val hdfs = FileSystem.newInstance(getConf)
    val workingPath = hdfs.makeQualified(new Path(arguments(0)))
    if (!hdfs.exists(workingPath)) throw new Exception("Input path [" + workingPath + "] does not exist")
    if (Log.isInfoEnabled) Log.info("Working path [" + workingPath + "] validated")
    testPath = new Path(workingPath, TestDir)
    trainPath = new Path(workingPath, TrainDir)
    modelPath = new Path(workingPath, ModelDir)
    SUCCESS
  }

  override def parameters() = {
    Array("working-path")
  }

  override def execute(): Int = {
    addResult(Model.build(FileSystem.newInstance(getConf), properties.getProperty("application.version"),
      trainPath.toString, testPath.toString, modelPath.toString).getOrElse(ModelPmml.EmptyModel))
    SUCCESS
  }

  def main(arguments: Array[String]): Unit = {
    System.exit(new Driver().runner(arguments: _*))
  }

}
