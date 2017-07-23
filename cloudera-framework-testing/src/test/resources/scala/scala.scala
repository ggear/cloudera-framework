import java.io.PrintStream

import com.cloudera.framework.assembly.ScriptUtil
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.io.Source

if (System.getenv("KILL_MY_SCRIPT") != null) throw new Exception("I have been asked to kill myself")

val path = new Path("/tmp/data.csv")
val hdfs = FileSystem.newInstance(ScriptUtil.getHadoopConf)
new PrintStream(hdfs.create(hdfs.makeQualified(path))) {
  print(Source.fromFile("../../../src/test/resources/scala/data.csv").mkString)
  close()
}
assert(hdfs.exists(path))
