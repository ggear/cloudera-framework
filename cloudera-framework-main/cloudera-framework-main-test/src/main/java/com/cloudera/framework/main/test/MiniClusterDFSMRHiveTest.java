package com.cloudera.framework.main.test;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.CopyTask;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MiniClusterDFSMRHiveTest extends BaseTest {

  private static final String COMMAND_DELIMETER = ";";

  private static Logger LOG = LoggerFactory
      .getLogger(MiniClusterDFSMRHiveTest.class);

  private MiniHS2 cluster;
  private HiveConf clusterConfig;

  @Override
  public FileSystem getFileSystem() throws IOException {
    return cluster.getLocalFS();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    cluster = new MiniHS2(clusterConfig = new HiveConf(new Configuration(),
        CopyTask.class), true);
    Map<String, String> config = new HashMap<String, String>();
    config.put(ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    config.put(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
    cluster.start(config);
    SessionState.start(new SessionState(clusterConfig));
    for (String table : processStatement("SHOW TABLES")) {
      processStatement("DROP TABLE " + table);
    }
  }

  @After
  @Override
  public void tearDown() throws Exception {
    cluster.stop();
  }

  public List<String> processStatement(String statement) throws SQLException,
      CommandNeedRetryException, IOException {
    List<String> results = new ArrayList<String>();
    CommandProcessor commandProcessor = CommandProcessorFactory
        .getForHiveCommand(statement.trim().split("\\s+"), clusterConfig);
    (commandProcessor = commandProcessor == null ? new Driver(clusterConfig)
        : commandProcessor).run(statement);
    if (commandProcessor instanceof Driver) {
      ((Driver) commandProcessor).getResults(results);
    }
    return results;
  }

  public List<String> processStatement(String directory, String file)
      throws SQLException, CommandNeedRetryException, IOException {
    List<String> results = new ArrayList<String>();
    for (String statement : readFileToLines(directory, file, COMMAND_DELIMETER)) {
      results.addAll(processStatement(statement));
    }
    return results;
  }

  private List<String> readFileToLines(String directory, String file,
      String delimeter) throws IOException {
    List<String> lines = new ArrayList<String>();
    InputStream inputStream = MiniClusterDFSMRHiveTest.class
        .getResourceAsStream(directory + "/" + file);
    if (inputStream != null) {
      try {
        for (String line : IOUtils.toString(inputStream).split(delimeter)) {
          if (!line.trim().equals("")) {
            lines.add(line.trim());
          }
        }
        return lines;
      } finally {
        inputStream.close();
      }
    }
    throw new IOException("Could not load file [" + directory + "/" + file
        + "] from classpath");
  }

}
