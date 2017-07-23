/*
***
# **PRE-PROCESSED SCRIPT - EDITS WILL BE CLOBBERED BY MAVEN BUILD**

This file is in the *SCRIPT* pre-processed state with template available by the
same package and file name under the modules src/main/template directory.

When editing the template directly (as indicated by the presence of the
TEMPLATE.PRE-PROCESSOR.RAW_TEMPLATE tag at the top of this file), care should
be taken to ensure the maven-resources-plugin generate-sources filtering of the
TEMPLATE.PRE-PROCESSOR tags, which comment and or uncomment blocks of the
template, leave the file in a consistent state, as a script ot library, post filtering.

It is desirable that in template form, the file remains both compilable and
runnable as a script in your IDEs (eg Eclipse, IntelliJ, CDSW etc). To setup
your environment, it may be necessary to run the pre-processed script once
(eg to execute AddJar commands with dependency versions completely resolved) but
from then on the template can be used for direct editing and distribution via
the source code control system and maven repository for dependencies.

The library can be tested during the standard maven compile and test phases.

Note that pre-processed files will be overwritten as part of the Maven build
process. Care should be taken to either ignore and not edit these files (eg
libraries) or check them in and note changes post Maven build (eg scripts)
***
*/

/*
%AddJar http://52.63.86.162/artifactory/cloudera-framework-releases/com/jag/maven-templater/maven-templater-assembly/1.2.3/maven-templater-assembly-1.2.3.jar
*/

/*
**IGNORE LIBRARY BOILERPLATE - START**

package com.cloudera.framework.example.three

object Model {

  def build(hdfs: org.apache.hadoop.fs.FileSystem, version: String, trainPath: String, testPath: String, modelPath: String): Option[String] = {

**IGNORE LIBRARY BOILERPLATE - FINISH**
*/

//
// Add dependencies dynamically
//
//@formatter:off
kernel.magics.addJar(
com.jag.maven.templater.TemplaterUtil.getDepJar(
  "com.cloudera.framework.example", "cloudera-framework-example-3", "1.5.6-cdh5.12.0-SNAPSHOT", "",
  "http://52.63.86.162/artifactory/cloudera-framework-releases",
  "http://52.63.86.162/artifactory/cloudera-framework-snapshots")
//
    + " -f")
//@formatter:on
//

import java.io.PrintStream

import com.cloudera.framework.assembly.ScriptUtil
import com.cloudera.framework.example.three.{Driver, ModelPmml}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession

import scala.util.Random

//
// Provide example parameters
val version = "0.0.1-CDSW"
val trainPath = "/tmp/roomsensors/train"
val testPath = "/tmp/roomsensors/test"
val modelPath = "/tmp/roomsensors/model"
//

/*
***
Build a model to predict if a hotel room is occupied or not based on data collected from sensors across the hotels rooms
***
*/
val hdfs = FileSystem.newInstance(ScriptUtil.getHadoopConf)
val sparkSession = SparkSession.builder().config(ScriptUtil.getSparkConf).getOrCreate()
var pmmlString = None: Option[String]

try {

  // Initialise HDFS
  if (!hdfs.exists(new Path(testPath))) hdfs.mkdirs(new Path(testPath))
  if (!hdfs.exists(new Path(trainPath))) hdfs.mkdirs(new Path(trainPath))
  if (hdfs.exists(new Path(modelPath))) hdfs.delete(new Path(modelPath), true)
  hdfs.mkdirs(new Path(modelPath))

  //
  // Load data
  ScriptUtil.copyFromUrl(hdfs, new Path(testPath, "sample.csv"), "https://raw.githubusercontent.com/ggear/cloudera-framework/master" +
    "/cloudera-framework-example/cloudera-framework-example-3/src/test/resources/data/roomsensors/test/sample/sample.csv")
  ScriptUtil.copyFromUrl(hdfs, new Path(trainPath, "sample.csv"), "https://raw.githubusercontent.com/ggear/cloudera-framework/master" +
    "/cloudera-framework-example/cloudera-framework-example-3/src/test/resources/data/roomsensors/train/sample/sample.csv")
  //

  if (hdfs.listFiles(new Path(testPath), true).hasNext && hdfs.listFiles(new Path(trainPath), true).hasNext) {

    // Load the training data
    val training = sparkSession.read.
      option("inferSchema", value = true).
      option("header", value = true).
      csv(hdfs.makeQualified(new Path(trainPath)).toUri.toString).
      drop("Date").cache()

    // Train a model
    val assembler = new VectorAssembler().
      setInputCols(training.columns.filter(_ != "Occupancy")).
      setOutputCol("featureVec")
    val logisticRegression = new LogisticRegression().
      setFeaturesCol("featureVec").
      setLabelCol("Occupancy").
      setRawPredictionCol("rawPrediction")
    val pipeline = new Pipeline().setStages(Array(assembler, logisticRegression))

    // Tune the model
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

    // Model parameters
    training.columns.zip(logisticRegressionModel.coefficients.toArray)

    // Model hyper-parameters
    logisticRegressionModel.getElasticNetParam
    logisticRegressionModel.getRegParam

    // Model validations
    validatorModel.validationMetrics.max

    // Convert model to PMML
    pmmlString = ModelPmml.export(hdfs, version, testPath, modelPath, training.schema, pipelineModel)

    // Write PMML to HDFS
    if (pmmlString.isDefined)
      new PrintStream(hdfs.create(new Path(modelPath, Driver.ModelFile))) {
        print(pmmlString.get)
        close()
      }

  }

  //
  // Assert model was successfully built
  if (pmmlString.isEmpty || !hdfs.exists(new Path(modelPath, Driver.ModelFile))) throw new AssertionError("Failed to build model")
  //

} finally {
  sparkSession.close()
  hdfs.close()
}

// Return model
pmmlString.getOrElse(ModelPmml.EmptyModel)
print(pmmlString.getOrElse(ModelPmml.EmptyModel))
pmmlString

/*
**IGNORE LIBRARY BOILERPLATE - START**

  }
}

**IGNORE LIBRARY BOILERPLATE - FINISH**
*/