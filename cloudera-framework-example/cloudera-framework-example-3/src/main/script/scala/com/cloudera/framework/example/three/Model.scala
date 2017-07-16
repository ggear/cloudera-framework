/**
  * ${script2source.RAW_EDIT_AS_SCRIPT}
  *
  * Warning, this file is in a ${script2source.RAW} state and should only be edited
  * if RAW and not already FILTERED. If RAW, this source should be edited as a script
  * but conscious that the Maven resources plugin acts as pre-processor filtering
  * each ${script2source.*} tag, commenting and or un-commenting the library specific
  * code blocks, which wrap the script within an object/function so it may act as a
  * Scala library.
  *
  * As a Cloudera Data Science Workbench script, this file should remain executable after
  * each of the dependencies have been added as below (note that filtering is required to
  * resolve the dependency ${*.version}'s, please see the library filtered source for
  * details: src/main/scala/com/cloudera/framework/example/three/Model.scala)
  *
  * As a Scala library, the Scala compiler will ensure consistency during a Maven build.
  *
  * This code was originally adapted from Sean Owen's project available at
  * https://github.com/srowen/cdsw-simple-serving
  *
  **/

/*
%AddJar https://repo.maven.apache.org/maven2/org/apache/commons/commons-csv/${commonscsv.version}/commons-csv-${commonscsv.version}.jar
%AddJar https://repo.maven.apache.org/maven2/org/jpmml/jpmml-sparkml/${sparkpmml.version}/jpmml-sparkml-${sparkpmml.version}.jar
%AddJar https://repo.maven.apache.org/maven2/org/jpmml/pmml-evaluator/${pmml.version}/pmml-evaluator-${pmml.version}.jar
*/

/* ${script2source.uncomment.close}

package com.cloudera.framework.example.three

import org.apache.hadoop.conf.Configuration
import org.dmg.pmml.PMML

object Model {

  def build(version: String, conf: Configuration, trainPath: String, testPath: String, modelPath: String): PMML = {

${script2source.uncomment.open} */

import java.io.{BufferedReader, InputStreamReader}
import javax.xml.transform.stream.StreamResult

import org.apache.commons.csv.CSVFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession
import org.dmg.pmml.{Application, Extension, FieldName, PMML}
import org.jpmml.evaluator.{ModelEvaluatorFactory, ProbabilityDistribution}
import org.jpmml.model.JAXBUtil
import org.jpmml.sparkml.ConverterUtil

import scala.collection.JavaConverters._
import scala.util.Random

// ${script2source.comment.open}

// Provide example parameters
val version = "0.0.1-CDSW"
val trainPath = "/tmp/roomsensors/train"
val testPath = "/tmp/roomsensors/test"
val modelPath = "/tmp/roomsensors/model"
val conf = new org.apache.hadoop.conf.Configuration()

// ${script2source.comment.close}
val hdfs = FileSystem.newInstance(conf)
val sparkSession = SparkSession.builder().config(new SparkConf).getOrCreate()
var pmml: PMML = null

try {

  // Initialise HDFS
  if (!hdfs.exists(new Path(testPath))) hdfs.mkdirs(new Path(testPath))
  if (!hdfs.exists(new Path(trainPath))) hdfs.mkdirs(new Path(trainPath))
  if (hdfs.exists(new Path(modelPath))) hdfs.delete(new Path(modelPath), true)
  hdfs.mkdirs(new Path(modelPath))

  if (hdfs.listFiles(new Path(testPath), true).hasNext && hdfs.listFiles(new Path(trainPath), true).hasNext) {

    // Load the training data from raw source
    val training = sparkSession.read.
      option("inferSchema", value = true).
      option("header", value = true).
      csv(trainPath).
      drop("Date").cache()

    // Train a logistic regression model
    val assembler = new VectorAssembler().
      setInputCols(training.columns.filter(_ != "Occupancy")).
      setOutputCol("featureVec")
    val logisticRegression = new LogisticRegression().
      setFeaturesCol("featureVec").
      setLabelCol("Occupancy").
      setRawPredictionCol("rawPrediction")
    val pipeline = new Pipeline().setStages(Array(assembler, logisticRegression))

    // Tune the logistic regression model
    val tuning = new ParamGridBuilder().
      addGrid(logisticRegression.regParam, Seq(0.00001, 0.001, 0.1)).
      addGrid(logisticRegression.elasticNetParam, Seq(1.0)).
      build()
    val evaluatorClassification = new BinaryClassificationEvaluator().
      setLabelCol("Occupancy").
      setRawPredictionCol("rawPrediction")
    val validator = new TrainValidationSplit().
      setSeed(Random.nextLong()).
      setEstimator(pipeline).
      setEvaluator(evaluatorClassification).
      setEstimatorParamMaps(tuning).
      setTrainRatio(0.9)
    val validatorModel = validator.fit(training)
    val pipelineModel = validatorModel.bestModel.asInstanceOf[PipelineModel]
    val logisticRegressionModel = pipelineModel.stages.last.asInstanceOf[LogisticRegressionModel]

    // Logistic regression model parameters
    training.columns.zip(logisticRegressionModel.coefficients.toArray).foreach(println)

    // Model hyperparameters
    logisticRegressionModel.getElasticNetParam
    logisticRegressionModel.getRegParam

    // Validation metric (accuracy)
    validatorModel.validationMetrics.max

    // Build PMML
    pmml = ConverterUtil.toPMML(training.schema, pipelineModel)

    // TODO: Does not work on CDSW
    //pmml.getHeader.setModelVersion(version)

    pmml.getHeader.setApplication(new Application("Occupancy Detection"))

    // Verify model
    val evaluator = ModelEvaluatorFactory.newInstance().newModelEvaluator(pmml)
    evaluator.verify()

    // Test accuracy of model based on test data
    var total = 0
    var correct = 0
    val testFiles = hdfs.listFiles(new Path(testPath), true)
    while (testFiles.hasNext) {
      val testStream = hdfs.open(testFiles.next().getPath)
      try {
        CSVFormat.RFC4180.withFirstRecordAsHeader().parse(new BufferedReader(new InputStreamReader(testStream)))
          .asScala.foreach { record =>
          val inputMap = record.toMap.asScala.
            filterKeys(_ != "Date").
            filterKeys(_ != "Occupancy").
            map { case (field, fieldValue) => (new FieldName(field), fieldValue) }.asJava
          val outputMap = evaluator.evaluate(inputMap)
          val expected = record.get("Occupancy").toInt
          val actual = outputMap.get(new FieldName("Occupancy")).
            asInstanceOf[ProbabilityDistribution].getResult.toString.toInt
          if (expected == actual) {
            correct += 1
          }
          total += 1
        }
      } finally {
        testStream.close()
      }
    }
    val accuracy = correct.toDouble / total

    // Store accuracy in PMML header
    val accuracyExtension = new Extension()
    accuracyExtension.setName("Accuracy")
    accuracyExtension.addContent("" + accuracy)
    pmml.getHeader.addExtensions(accuracyExtension)

    // Export PMML to HDFS
    val pmmlOutputStream = hdfs.create(new Path(modelPath, "occupancy.pmml"))
    try {
      JAXBUtil.marshalPMML(pmml, new StreamResult(pmmlOutputStream))
    } finally {
      pmmlOutputStream.close()
    }

  }

  pmml

} finally {
  sparkSession.close()
  hdfs.close()
}

/* ${script2source.uncomment.close}

  }
}

${script2source.uncomment.open} */