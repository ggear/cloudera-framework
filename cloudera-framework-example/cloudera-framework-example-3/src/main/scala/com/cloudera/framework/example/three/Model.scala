/*
 * This code was originally adapted from Sean Owen's project available at
 * https://github.com/srowen/cdsw-simple-serving
 */

package com.cloudera.framework.example.three

import org.apache.hadoop.conf.Configuration

/**
  * The object definition here is boilerplate to allow the project to be built and
  * is not directly used in a Cloudera Data Science Workbench (CDSW) session.
  */
object Model {

  def build(conf: Configuration, rawPath: String, trainPath: String, modelPath: String): Unit = {

    ///////////////////////////////////////////////////////////////////////////////
    //
    // Start CDSW session
    //
    // conf: Configuration
    // rawPath: String
    // trainPath: String
    // modelPath: String
    //
    ///////////////////////////////////////////////////////////////////////////////

    import javax.xml.transform.stream.StreamResult

    import org.apache.hadoop.fs.{FileSystem, Path}
    import org.apache.spark.SparkConf
    import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
    import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
    import org.apache.spark.ml.feature.VectorAssembler
    import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
    import org.apache.spark.ml.{Pipeline, PipelineModel}
    import org.apache.spark.sql.SparkSession
    import org.dmg.pmml.Application
    import org.jpmml.model.JAXBUtil
    import org.jpmml.sparkml.ConverterUtil

    import scala.util.Random

    val sparkSession = SparkSession.builder().config(new SparkConf).getOrCreate()

    import sparkSession.implicits._

    try {

      // Prepare training data from raw source
      sparkSession.read.textFile(rawPath).
        map { line =>
          if (line.startsWith("\"date\"")) {
            line
          } else {
            line.substring(line.indexOf(',') + 1)
          }
        }.
        repartition(1).
        write.text(trainPath)
      val training = sparkSession.read.
        option("inferSchema", value = true).
        option("header", value = true).
        csv(trainPath).
        drop("date").cache()

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
      val evaluator = new BinaryClassificationEvaluator().
        setLabelCol("Occupancy").
        setRawPredictionCol("rawPrediction")
      val validator = new TrainValidationSplit().
        setSeed(Random.nextLong()).
        setEstimator(pipeline).
        setEvaluator(evaluator).
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

      // Export model as PMML to HDFS
      val pmml = ConverterUtil.toPMML(training.schema, pipelineModel)
      pmml.getHeader.setApplication(new Application("Occupancy Detection"))
      val pmmlStream = FileSystem.newInstance(conf).create(new Path(modelPath, "occupancy.pmml"))
      try {
        JAXBUtil.marshalPMML(pmml, new StreamResult(pmmlStream))
      } finally {
        pmmlStream.close()
      }

    } finally {
      sparkSession.close()
    }

    ///////////////////////////////////////////////////////////////////////////////
    //
    // End the CDSW session
    //
    ///////////////////////////////////////////////////////////////////////////////

  }

  def accuracy(conf: Configuration, testPath: String, modelPath: String): Double = {

    ///////////////////////////////////////////////////////////////////////////////
    //
    // Start CDSW session
    //
    // conf: Configuration
    // testPath: String
    // modelPath: String
    //
    ///////////////////////////////////////////////////////////////////////////////

    import java.io.{BufferedReader, InputStreamReader}

    import org.apache.commons.csv.CSVFormat
    import org.apache.hadoop.fs.{FileSystem, Path}
    import org.dmg.pmml.FieldName
    import org.jpmml.evaluator.{ModelEvaluatorFactory, ProbabilityDistribution}
    import org.jpmml.model.PMMLUtil

    import scala.collection.JavaConverters._

    // Get model from HDFS
    val pmmlStream = FileSystem.newInstance(conf).open(new Path(modelPath, "occupancy.pmml"))
    val pmml =
      try {
        PMMLUtil.unmarshal(pmmlStream)
      } finally {
        pmmlStream.close()
      }

    // Verify model
    val evaluator = ModelEvaluatorFactory.newInstance().newModelEvaluator(pmml)
    evaluator.verify()

    // Test accuracy of model based on test data
    var total = 0
    var correct = 0
    val testStream = FileSystem.newInstance(conf).open(new Path(testPath, "sample.txt"))
    try {
      CSVFormat.RFC4180.withFirstRecordAsHeader().parse(new BufferedReader(new InputStreamReader(testStream)))
        .asScala.foreach { record =>
        val inputMap = record.toMap.asScala.
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

    return correct.toDouble / total

    ///////////////////////////////////////////////////////////////////////////////
    //
    // End the CDSW session
    //
    ///////////////////////////////////////////////////////////////////////////////

  }

}
