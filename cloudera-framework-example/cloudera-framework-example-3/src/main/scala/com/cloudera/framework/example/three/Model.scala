/*
 * This code was originally adapted from Sean Owen's project available at
 * https://github.com/srowen/cdsw-simple-serving
 */

package com.cloudera.framework.example.three

import java.io.OutputStream

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

    var hdfs: FileSystem = null
    var pmmlOutputStream: OutputStream = null
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
      hdfs = FileSystem.newInstance(conf)
      pmmlOutputStream = hdfs.create(new Path(modelPath, "occupancy.pmml"))
      JAXBUtil.marshalPMML(pmml, new StreamResult(pmmlOutputStream))

      ///////////////////////////////////////////////////////////////////////////////
      //
      // End the CDSW session
      //
      ///////////////////////////////////////////////////////////////////////////////

    }
    finally {
      if (pmmlOutputStream != null) pmmlOutputStream.close()
      if (hdfs != null) hdfs.close()
      sparkSession.close()
    }
  }

}
