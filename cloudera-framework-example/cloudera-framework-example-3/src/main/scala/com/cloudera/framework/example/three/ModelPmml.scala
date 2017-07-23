package com.cloudera.framework.example.three

import java.io.{BufferedReader, InputStreamReader, StringWriter}
import javax.xml.transform.stream.StreamResult

import org.apache.commons.csv.CSVFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types.StructType
import org.dmg.pmml.{Application, Extension, FieldName, PMML}
import org.jpmml.evaluator.{ModelEvaluatorFactory, ProbabilityDistribution}
import org.jpmml.model.JAXBUtil
import org.jpmml.sparkml.ConverterUtil
import org.jpmml.sparkml.feature.VectorAssemblerConverter
import org.jpmml.sparkml.model.LogisticRegressionModelConverter

import scala.collection.JavaConverters._

/**
  * Provide PMML operations for the [com.cloudera.framework.example.three.Model].
  *
  * It is necessary that this implementation be external to the [com.cloudera.framework.example.three.Model] script
  * so that the com.google.*, org.jpmml.* and org.dmg.* namespaces can be shaded from the incompatible versions
  * shipped with Spark. See https://issues.apache.org/jira/browse/SPARK-15526.
  */
object ModelPmml {

  val EmptyModel = "<PML/>"

  val MinimumAccuracy = 0.9

  // Ensure that the converter mappings remain consistent post shading
  ConverterUtil.putConverterClazz(classOf[VectorAssembler], classOf[VectorAssemblerConverter])
  ConverterUtil.putConverterClazz(classOf[LogisticRegressionModel], classOf[LogisticRegressionModelConverter])

  def export(hdfs: FileSystem, version: String, testPath: String, modelPath: String, schema: StructType, model: PipelineModel):
  Option[String] = {

    var pmmlString = None: Option[String]

    // Model to PMML
    val pmml = ConverterUtil.toPMML(schema, model)
    pmml.getHeader.setModelVersion(version)
    pmml.getHeader.setApplication(new Application("Occupancy Detection"))

    // Model verify
    val evaluator = ModelEvaluatorFactory.newInstance().newModelEvaluator(pmml)
    evaluator.verify()

    // Model accuracy test
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

    // Require a minimum accuracy to verify model
    if (accuracy >= MinimumAccuracy) {

      // Store accuracy in PMML header
      val accuracyExtension = new Extension()
      accuracyExtension.setName("Accuracy")
      accuracyExtension.addContent("" + accuracy)
      pmml.getHeader.addExtensions(accuracyExtension)

      // Export PMML to String
      val stringWriter = new StringWriter()
      try {
        JAXBUtil.marshalPMML(pmml, new StreamResult(stringWriter))
      } finally {
        stringWriter.close()
      }

      pmmlString = Some(stringWriter.toString)
    }

    pmmlString

  }

}