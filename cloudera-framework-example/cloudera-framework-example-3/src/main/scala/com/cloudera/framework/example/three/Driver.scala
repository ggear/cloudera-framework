package com.cloudera.framework.example.three

import java.io.StringWriter
import java.util.Properties
import javax.xml.transform.stream.StreamResult

import com.cloudera.framework.common.Driver.RETURN_SUCCESS
import com.cloudera.framework.example.three.Driver.{ModelDir, TestDir, TrainDir}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.jpmml.model.JAXBUtil
import org.slf4j.LoggerFactory

import scala.io.Source

object Driver {

  val TestDir = "test"
  val TrainDir = "train"
  val ModelDir = "model"

}

class Driver extends com.cloudera.framework.common.Driver {

  val properties = new Properties()
  private val Log = LoggerFactory.getLogger(classOf[Driver])
  var testPath: Path = _
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
    properties.load(Source.fromURL(getClass.getResource("/application.properties")).bufferedReader())
    val hdfs = FileSystem.newInstance(getConf)
    val workingPath = hdfs.makeQualified(new Path(arguments(0)))
    if (!hdfs.exists(workingPath)) throw new Exception("Input path [" + workingPath + "] does not exist")
    if (Log.isInfoEnabled) Log.info("Working path [" + workingPath + "] validated")
    testPath = new Path(workingPath, TestDir)
    if (!hdfs.exists(testPath)) hdfs.mkdirs(testPath)
    trainPath = new Path(workingPath, TrainDir)
    if (!hdfs.exists(trainPath)) hdfs.mkdirs(trainPath)
    modelPath = new Path(workingPath, ModelDir)
    if (hdfs.exists(modelPath)) hdfs.delete(modelPath, true)
    hdfs.mkdirs(modelPath)
    RETURN_SUCCESS
  }

  override def parameters(): Array[String] = {
    Array("working-path")
  }

  override def execute(): Int = {
    val pmml = Model.build(properties.getProperty("application.version"), getConf, trainPath.toString, testPath.toString, modelPath.toString)
    addResult(pmml)
    val pmmlStringWriter = new StringWriter()
    JAXBUtil.marshalPMML(pmml, new StreamResult(pmmlStringWriter))
    addResult("\n\n" + pmmlStringWriter.toString)
    RETURN_SUCCESS
  }

}
