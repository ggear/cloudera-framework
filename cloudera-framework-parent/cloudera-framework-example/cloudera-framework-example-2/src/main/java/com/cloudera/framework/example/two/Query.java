package com.cloudera.framework.example.two;


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;

import com.cloudera.framework.common.DriverSpark;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Query extends DriverSpark {

  private static final Logger LOG = LoggerFactory.getLogger(Query.class);
  protected Path inputPath;
  private JavaSparkContext sparkContext;

  public Query(Configuration configuration) {
    super(configuration);
  }

  public static void main(String... arguments) {
    new Query(null).runner(arguments);
  }

  @Override
  public String description() {
    return "Query my dataset";
  }

  @Override
  public String[] parameters() {
    return new String[]{"input-path"};
  }

  @Override
  public int prepare(String... arguments) throws Exception {
    if (arguments == null || arguments.length != parameters().length) {
      throw new Exception("Invalid number of arguments");
    }
    FileSystem hdfs = FileSystem.newInstance(getConf());
    inputPath = hdfs.makeQualified(new Path(arguments[0]));
    if (LOG.isInfoEnabled()) {
      LOG.info("Input path [" + inputPath + "] validated");
    }
    sparkContext = new JavaSparkContext(new SparkConf());
    if (LOG.isInfoEnabled()) {
      LOG.info("Spark context created");
    }
    return SUCCESS;
  }

  @Override
  public int execute() throws Exception {
    SQLContext sqlContext = new SQLContext(sparkContext);
    DataFrame inputDataFrame = sqlContext.createDataFrame(
      sparkContext.textFile(inputPath.toString()).map(new CsvToRow()), DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("myday", DataTypes.StringType, true),
        DataTypes.createStructField("myint", DataTypes.IntegerType, true))
      ));
    inputDataFrame.registerTempTable("mytable");
    addResults(new ArrayList<>(sqlContext.sql(
      "SELECT myday, sum(myint) as myint FROM mytable WHERE myday is not NULL and myint is not NULL GROUP BY myday ORDER BY myint"
    ).javaRDD().map(new RowToTsv()).collect()));
    return SUCCESS;
  }

  @Override
  public int cleanup() {
    if (sparkContext != null) {
      sparkContext.close();
    }
    return SUCCESS;
  }

  private static class RowToTsv implements Function<Row, String> {
    public String call(Row row) {
      StringBuilder string = new StringBuilder();
      for (int i = 0; i < row.size(); i++) {
        string.append(row.get(i) == null ? "NULL" : row.get(i).toString());
        if (i + 1 < row.size()) {
          string.append("\t");
        }
      }
      return string.toString();
    }
  }

  private static class CsvToRow implements Function<String, Row> {
    public Row call(String record) throws Exception {
      String myday = null;
      Integer myint = null;
      String[] cells = record.split(",");
      if (cells.length > 1) {
        try {
          myday = new SimpleDateFormat("E").format(new SimpleDateFormat("yyyy-MM-dd").parse(cells[0]));
        } catch (Exception exception) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Could not format CSV cell [" + cells[0] + "] as a date", exception);
          }
        }
        try {
          myint = Integer.parseInt(cells[1]);
        } catch (Exception exception) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Could not format CSV cell [" + cells[1] + "] as an int", exception);
          }
        }
      }
      return RowFactory.create(myday, myint);
    }
  }

}
