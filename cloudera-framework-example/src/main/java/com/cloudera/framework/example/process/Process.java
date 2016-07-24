package com.cloudera.framework.example.process;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.cloudera.framework.common.Driver;
import com.cloudera.framework.example.Constants;
import com.cloudera.framework.example.model.RecordCounter;

public class Process extends Driver {

  private String pathRaw;
  private String pathStaged;
  private String pathPartitioned;
  private String pathCleansed;

  public Process() {
    super();
  }

  public Process(Configuration confguration) {
    super(confguration);
  }

  @Override
  public String description() {
    return "Process my dataset";
  }

  @Override
  public String[] options() {
    return new String[] {};
  }

  @Override
  public String[] parameters() {
    return new String[] { "input-path-raw", "input-path-staged", "input-path-partitioned", "input-path-cleansed" };
  }

  @Override
  public void reset() {
    super.reset();
    for (RecordCounter counter : Stage.COUNTERS) {
      incrementCounter(Stage.class.getCanonicalName(), counter, 0);
    }
    for (RecordCounter counter : Partition.COUNTERS) {
      incrementCounter(Partition.class.getCanonicalName(), counter, 0);
    }
    for (RecordCounter counter : Cleanse.COUNTERS) {
      incrementCounter(Cleanse.class.getCanonicalName(), counter, 0);
    }
  }

  @Override
  public int prepare(String... arguments) throws Exception {
    if (arguments == null || arguments.length != 4) {
      throw new Exception("Invalid number of arguments");
    }
    pathRaw = arguments[0];
    pathStaged = arguments[1];
    pathPartitioned = arguments[2];
    pathCleansed = arguments[3];
    return RETURN_SUCCESS;
  }

  @Override
  public int execute() throws InterruptedException, ExecutionException, IOException, ClassNotFoundException {
    int returnValue = RETURN_FAILURE_RUNTIME;
    Driver stageDriver = new Stage(getConf());
    Driver partitionDriver = new Partition(getConf());
    Driver cleanseDriver = new Cleanse(getConf());
    if ((returnValue = stageDriver
        .run(new String[] { pathRaw + Path.SEPARATOR_CHAR + Constants.DIR_REL_MYDS_CANONICAL, pathStaged })) == RETURN_SUCCESS) {
      if ((returnValue = partitionDriver
          .run(new String[] { pathStaged + Path.SEPARATOR_CHAR + Constants.DIR_REL_MYDS_CANONICAL, pathPartitioned })) == RETURN_SUCCESS) {
        returnValue = cleanseDriver
            .run(new String[] { pathPartitioned + Path.SEPARATOR_CHAR + Constants.DIR_REL_MYDS_CANONICAL, pathCleansed });
      }
    }
    importCountersAll(stageDriver.getCounters());
    importCountersAll(partitionDriver.getCounters());
    importCountersAll(cleanseDriver.getCounters());
    return returnValue;
  }

  @Override
  public int cleanup() throws IOException {
    return RETURN_SUCCESS;
  }

  public static void main(String... arguments) throws Exception {
    System.exit(new Process().runner(arguments));
  }

}
