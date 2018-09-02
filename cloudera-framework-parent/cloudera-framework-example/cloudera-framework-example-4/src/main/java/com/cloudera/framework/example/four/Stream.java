package com.cloudera.framework.example.four;


import com.cloudera.framework.common.DriverSpark;
import com.cloudera.labs.envelope.run.Runner;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Stream extends DriverSpark {

  private static final Logger LOG = LoggerFactory.getLogger(Stream.class);

  private String inputPath;
  private String outputPath;

  public Stream(Configuration configuration) {
    super(configuration);
  }

  public static void main(String... arguments) {
    new Stream(null).runner(arguments);
  }

  @Override
  public String description() {
    return "Stream my dataset";
  }

  @Override
  public String[] parameters() {
    return new String[]{"input-path", "output_path"};
  }

  @Override
  public int prepare(String... arguments) throws Exception {
    if (arguments == null || arguments.length != parameters().length) {
      throw new Exception("Invalid number of arguments");
    }
    inputPath = arguments[0];
    outputPath = arguments[1];
    return SUCCESS;
  }

  @Override
  public int execute() throws Exception {
    System.setProperty("DFS_INPUT", inputPath);
    System.setProperty("DFS_OUTPUT", outputPath);
    Runner.run(ConfigUtils.applySubstitutions(ConfigUtils.configFromResource("/envelope/csv_to_parquet.conf")));
    return SUCCESS;
  }

  @Override
  public void reset() {
    System.clearProperty("DFS_INPUT");
    System.clearProperty("DFS_OUTPUT");
  }

}
