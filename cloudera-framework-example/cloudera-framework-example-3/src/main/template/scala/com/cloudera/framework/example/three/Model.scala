/*
${TEMPLATE.PRE-PROCESSOR.SPACER}
${TEMPLATE.PRE-PROCESSOR.RAW_TEMPLATE}

This file is in the ${TEMPLATE.PRE-PROCESSOR.STATE} pre-processed state with template available by the
same package and file name under the modules src/main/template directory.

When editing the template directly (as indicated by the presence of the
TEMPLATE.PRE-PROCESSOR.RAW_TEMPLATE tag at the top of this file), care should
be taken to ensure the maven-resources-plugin generate-sources filtering of the
TEMPLATE.PRE-PROCESSOR tags, which comment and or uncomment blocks of the
template, leave the file in a consistent state post filtering.

It is desirable that in template form, the file remains both compilable and
runnable as a script in your IDEs (eg Eclipse, IntelliJ, CDSW etc). To setup
your environment, it may be necessary to run the pre-processed script once
(eg to execute AddJar commands with dependency versions completely resolved) but
from then on the template can be used for direct editing and distribution via
the source code control system.

The library can be tested during the standard maven compile and test phases.
${TEMPLATE.PRE-PROCESSOR.SPACER}
*/

/*
%AddJar https://repo.maven.apache.org/maven2/org/jpmml/pmml-evaluator/${pmml.version}/pmml-evaluator-${pmml.version}.jar
%AddJar https://repo.maven.apache.org/maven2/org/jpmml/jpmml-sparkml/${sparkpmml.version}/jpmml-sparkml-${sparkpmml.version}.jar
%AddJar https://repo.maven.apache.org/maven2/org/apache/commons/commons-csv/${commonscsv.version}/commons-csv-${commonscsv.version}.jar
*/

/*
${TEMPLATE.PRE-PROCESSOR.UNCLOSE}

package com.cloudera.framework.example.three

import org.apache.hadoop.conf.Configuration
import org.dmg.pmml.PMML

object Model {

  def build(version: String, conf: Configuration, trainPath: String, testPath: String, modelPath: String): PMML = {

${TEMPLATE.PRE-PROCESSOR.UNOPEN}
*/

import java.io.{BufferedReader, InputStreamReader, PrintStream}
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
import scala.io.Source
import scala.util.Random

//${TEMPLATE.PRE-PROCESSOR.OPEN}
// Provide example parameters
val version = "0.0.1-CDSW"
val trainPath = "/tmp/roomsensors/train"
val testPath = "/tmp/roomsensors/test"
val modelPath = "/tmp/roomsensors/model"
val conf = new org.apache.hadoop.conf.Configuration()
//${TEMPLATE.PRE-PROCESSOR.CLOSE}

/*
${TEMPLATE.PRE-PROCESSOR.SPACER}
Build a model to predict if a hotel room is occupied or not based on data collected from sensors accross the hotels rooms
${TEMPLATE.PRE-PROCESSOR.SPACER}
*/

val hdfs = FileSystem.newInstance(conf)
val sparkSession = SparkSession.builder().config(new SparkConf).getOrCreate()
var pmml: PMML = null

try {

  // Initialise HDFS
  if (!hdfs.exists(new Path(testPath))) hdfs.mkdirs(new Path(testPath))
  if (!hdfs.exists(new Path(trainPath))) hdfs.mkdirs(new Path(trainPath))
  if (hdfs.exists(new Path(modelPath))) hdfs.delete(new Path(modelPath), true)
  hdfs.mkdirs(new Path(modelPath))

  //${TEMPLATE.PRE-PROCESSOR.OPEN}
  // Load data
  val testDataPrintStream = new PrintStream(hdfs.create(new Path(testPath, "sample.csv")))
  try {
    testDataPrintStream.print(Source.fromURL("https://raw.githubusercontent.com/ggear/cloudera-framework/master" +
      "/cloudera-framework-example/cloudera-framework-example-3/src/test/resources/data/roomsensors/test/sample/sample.csv").mkString)
  } finally {
    testDataPrintStream.close()
  }
  val trainDataPrintStream = new PrintStream(hdfs.create(new Path(trainPath, "sample.csv")))
  try {
    trainDataPrintStream.print(Source.fromURL("https://raw.githubusercontent.com/ggear/cloudera-framework/master" +
      "/cloudera-framework-example/cloudera-framework-example-3/src/test/resources/data/roomsensors/train/sample/sample.csv").mkString)
  } finally {
    trainDataPrintStream.close()
  }
  //${TEMPLATE.PRE-PROCESSOR.CLOSE}

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

//${TEMPLATE.PRE-PROCESSOR.OPEN}
// Assert accuracy of the model
try {
  val accuracy = pmml.getHeader.getExtensions.get(0).getContent.get(0).toString.toDouble
  if (accuracy < 0.9) throw new AssertionError("Model has accuracy [" + accuracy + "] less than required threshold [0.9]")
} catch {
  case t: Throwable =>
    throw new AssertionError("Model build failed, could not determine accuracy", t)
}
//${TEMPLATE.PRE-PROCESSOR.CLOSE}

/*
${TEMPLATE.PRE-PROCESSOR.UNCLOSE}

  }
}

${TEMPLATE.PRE-PROCESSOR.UNOPEN}
*/