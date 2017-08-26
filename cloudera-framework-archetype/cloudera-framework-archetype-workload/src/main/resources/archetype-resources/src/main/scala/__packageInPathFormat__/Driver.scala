/**
  * This is a trivial example [[com.cloudera.framework.common.Driver]], please replace with your implementation.
  *
  * More extensive examples are bundled with the cloudera-framework source here:
  * https://github.com/ggear/cloudera-framework/tree/master/cloudera-framework-parent/cloudera-framework-example
  */
#* // @formatter:off
*#
package ${package}

import Driver.Name
import com.cloudera.framework.common.Driver.Counter.{RECORDS_IN, RECORDS_OUT}
import com.cloudera.framework.common.Driver.{Engine, FAILURE_ARGUMENTS, SUCCESS}
import com.cloudera.mytest.Driver.PathInput
import com.cloudera.mytest.Driver.PathOutput
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Driver implementation
  */
class Driver(configuration: Configuration) extends com.cloudera.framework.common.DriverSpark(configuration) {

  // Input DFS path
  var rootPath: Path = _

  /**
    * Validate input path parameter
    */
  override def prepare(arguments: String*): Int = {
    if (arguments == null || arguments.length != parameters().length) return FAILURE_ARGUMENTS
    rootPath = FileSystem.newInstance(getConf).makeQualified(new Path(arguments(0)))
    SUCCESS
  }

  /**
    * Define input path parameter
    */
  override def parameters() = {
    Array("root-path")
  }

  /**
    * Execute the driver against the input path
    */
  override def execute(): Int = {

    // Create the Spark session
    val spark = SparkSession.builder.config(new SparkConf).appName(Name).getOrCreate()

    // Read the input CSV into a DataFrame and set the input record count
    val input = spark.read.format("com.databricks.spark.csv").option("header", "true")
      .load(new Path(rootPath, PathInput).toString)
    incrementCounter(RECORDS_IN, input.count())

    // Transform the input DataFrame and set the output record count
    val output = input.drop("index").filter("opt_in = 'true' or opt_in = 'false'")
    incrementCounter(RECORDS_OUT, output.count())

    // Write the output DataFrame to partitioned CSV files
    output.write.format("com.databricks.spark.csv").option("header", "true")
      .partitionBy("opt_in").save(new Path(rootPath, Driver.PathOutput).toString)

    // Close the session and return success
    spark.close()
    SUCCESS

  }

  /**
    * Main method to run the driver
    */
  def main(arguments: Array[String]): Unit = {
    System.exit(new Driver(null).runner(arguments: _*))
  }

}

/**
  * Driver constants
  */
object Driver {

  // Application name
  val Name = "${artifactId}"

  // Dataset paths
  val PathInput = "input"
  val PathOutput = "output"

}
