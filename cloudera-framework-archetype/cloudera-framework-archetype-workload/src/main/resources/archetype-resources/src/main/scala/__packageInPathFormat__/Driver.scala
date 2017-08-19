/**
  * This is a trivial example [[com.cloudera.framework.common.Driver]], please replace with your implementation.
  *
  * More detailed examples are available here:
  * https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-parent/cloudera-framework-example
  */
#* // @formatter:off
*#
package ${package}

import com.cloudera.framework.common.Driver.Counter.{RECORDS_IN, RECORDS_OUT}
import com.cloudera.framework.common.Driver.SUCCESS
import com.cloudera.mytest.Driver.PathInput
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Driver constants
  */
object Driver {

  // Application name
  val Name = "${artifactId}"

  // Dataset paths
  val PathInput = "input"
  var PathOutput = "output"

}

/**
  * Driver implementation
  */
class Driver extends com.cloudera.framework.common.Driver {

  // Input DFS path
  var rootPath: Path = _

  /**
    * Constructor to take configuration
    */
  def this(configuration: Configuration) {
    this
    super.setConf(configuration)
  }

  /**
    * Define input path parameter
    */
  override def parameters(): Array[String] = {
    Array("root-path")
  }

  /**
    * Validate input path parameter
    */
  override def prepare(arguments: String*): Int = {
    if (arguments == null || arguments.length != parameters().length) throw new Exception("Invalid number of arguments")
    rootPath = FileSystem.newInstance(getConf).makeQualified(new Path(arguments(0)))
    SUCCESS
  }

  /**
    * Execute the driver against the input path
    */
  override def execute(): Int = {

    // Create the Spark session
    val sparkSession = SparkSession.builder.config(new SparkConf).appName(Driver.Name).getOrCreate()

    // Read the input CSV into a dataframe and set the record count
    val inputDataFrame = sparkSession.read.format("com.databricks.spark.csv").option("header", "true")
      .load(new Path(rootPath, PathInput).toString)
    incrementCounter(RECORDS_IN, inputDataFrame.count())

    // Transform the input dataframe and write to partitioned files
    inputDataFrame.drop("index").write.format("com.databricks.spark.csv").option("header", "true")
      .partitionBy("sex").save(new Path(rootPath, Driver.PathOutput).toString)

    // Read the output dataframe and set the record count
    val outputDataFrame = sparkSession.read.format("com.databricks.spark.csv").option("header", "true")
      .load(new Path(rootPath, PathInput).toString)
    incrementCounter(RECORDS_OUT, outputDataFrame.count())

    // Close the session and return success
    sparkSession.close()
    SUCCESS
  }

  /**
    * Main method to run the driver
    */
  def main(arguments: Array[String]): Unit = {
    System.exit(new Driver().runner(arguments: _*))
  }

}
