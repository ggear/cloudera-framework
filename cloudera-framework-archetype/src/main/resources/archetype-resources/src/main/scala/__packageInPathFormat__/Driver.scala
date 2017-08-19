/**
  * This is an example Driver, please replace with your implementation.
  */
#* // @formatter:off
*#
package ${package}

import com.cloudera.framework.common.Driver.RETURN_SUCCESS
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Driver {
  val Name = "${artifactId}"
}

class Driver extends com.cloudera.framework.common.Driver {

  var inputPath: Path = _

  def this(configuration: Configuration) {
    this
    super.setConf(configuration)
  }

  override def parameters(): Array[String] = {
    Array("input-path")
  }

  override def prepare(arguments: String*): Int = {
    if (arguments == null || arguments.length != parameters().length) throw new Exception("Invalid number of arguments")
    inputPath = FileSystem.newInstance(getConf).makeQualified(new Path(arguments(0)))
    RETURN_SUCCESS
  }

  override def execute(): Int = {
    val sparkSession = SparkSession.builder.config(new SparkConf).appName(Driver.Name).getOrCreate()
    addResult(sparkSession.read.csv(inputPath.toString).count())
    sparkSession.close()
    RETURN_SUCCESS
  }

  def main(arguments: Array[String]): Unit = {
    System.exit(new Driver().runner(arguments))
  }

}
