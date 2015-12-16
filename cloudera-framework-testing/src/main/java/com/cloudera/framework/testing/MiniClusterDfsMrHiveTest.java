package com.cloudera.framework.testing;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.CopyTask;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for all local-cluster DFS, MR and Hive tests, single-process,
 * multi-threaded DFS, MR and Hive daemons, exercises the full read/write path
 * of the stack providing isolated and idempotent runtime
 */
public class MiniClusterDfsMrHiveTest extends BaseTest {

  private static final String COMMAND_DELIMETER = ";";

  private static final int MAX_RESULTS_DEFAULT = 100;

  private static Logger LOG = LoggerFactory.getLogger(MiniClusterDfsMrHiveTest.class);

  private static HiveConf conf;
  private static MiniHS2 miniHs2;
  private static FileSystem fileSystem;

  public MiniClusterDfsMrHiveTest() {
    super();
  }

  public MiniClusterDfsMrHiveTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets,
      String[][][] labels, @SuppressWarnings("rawtypes") Map[] metadata) {
    super(sources, destinations, datasets, subsets, labels, metadata);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public FileSystem getFileSystem() {
    return fileSystem;
  }

  /**
   * Process a <code>statement</code>
   *
   * @param statement
   * @return {@link List} of {@link String} results, no result will be indicated
   *         by 1-length empty {@link String} {@link List}
   * @throws Exception
   */
  public static List<String> processStatement(String statement) throws Exception {
    return processStatement(statement, Collections.<String, String> emptyMap(),
        Collections.<String, String> emptyMap());
  }

  /**
   * Process a <code>statement</code>, making <code>hivevar</code> substitutions
   * from <code>parameters</code>
   *
   * @param statement
   * @param parameters
   * @return {@link List} of {@link String} results, no result will be indicated
   *         by 1-length empty {@link String} {@link List}
   * @throws Exception
   */
  public static List<String> processStatement(String statement, Map<String, String> parameters) throws Exception {
    return processStatement(statement, parameters, Collections.<String, String> emptyMap());
  }

  /**
   * Process a <code>statement</code>, making <code>hivevar</code> substitutions
   * from <code>parameters</code> and session settings from
   * <code>configuration</code>
   *
   * @param statement
   * @param parameters
   * @param configuration
   * @return {@link List} of {@link String} results, no result will be indicated
   *         by 1-length empty {@link String} {@link List}
   * @throws Exception
   */
  public static List<String> processStatement(String statement, Map<String, String> parameters,
      Map<String, String> configuration) throws Exception {
    return processStatement(statement, parameters, configuration, MAX_RESULTS_DEFAULT);
  }

  /**
   * Process a <code>statement</code>, making <code>hivevar</code> substitutions
   * from <code>parameters</code> and session settings from
   * <code>configuration</code>
   *
   * @param statement
   * @param parameters
   * @param configuration
   * @param maxResults
   * @return {@link List} of {@link String} results, no result will be indicated
   *         by 1-length empty {@link String} {@link List}
   * @throws Exception
   */
  public static List<String> processStatement(String statement, Map<String, String> parameters,
      Map<String, String> configuration, int maxResults) throws Exception {
    long time = debugMessageHeader(LOG, "processStatement");
    HiveConf confSession = new HiveConf(conf);
    for (String key : configuration.keySet()) {
      confSession.set(key, configuration.get(key));
    }
    List<String> results = new ArrayList<String>();
    CommandProcessor commandProcessor = CommandProcessorFactory.getForHiveCommand(
        (statement = new StrSubstitutor(parameters, "${hivevar:", "}").replace(statement.trim())).split("\\s+"),
        confSession);
    if (commandProcessor == null) {
      ((Driver) (commandProcessor = new Driver(confSession))).setMaxRows(maxResults);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(LOG_PREFIX + " [processStatement] hive exec:\n" + statement);
    }
    String responseErrorMessage = null;
    int responseCode = commandProcessor.run(statement).getResponseCode();
    if (commandProcessor instanceof Driver) {
      ((Driver) commandProcessor).getResults(results);
      responseErrorMessage = ((Driver) commandProcessor).getErrorMsg();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(LOG_PREFIX + " [processStatement] hive results [" + results.size()
          + (results.size() == maxResults ? " (MAX)" : "") + "]:\n" + StringUtils.join(results.toArray(), "\n"));
    }
    debugMessageFooter(LOG, "processStatement", time);
    if (responseCode != 0 || responseErrorMessage != null) {
      throw new SQLException("Statement executed with error response code [" + responseCode + "]"
          + (responseErrorMessage != null ? " and error message [" + responseErrorMessage + " ]" : ""));
    }
    return results;
  }

  /**
   * Process a set of <code>;</code> delimited statements from a
   * <code>file</code> in a <code>directory</code>
   *
   * @param directory
   * @param file
   * @return {@link List} of {@link List} of {@link String} results per
   *         statement, no result will be indicated by 1-length empty
   *         {@link String} {@link List}
   * @throws Exception
   */
  public static List<List<String>> processStatement(String directory, String file) throws Exception {
    return processStatement(directory, file, Collections.<String, String> emptyMap(),
        Collections.<String, String> emptyMap());
  }

  /**
   * Process a set of <code>;</code> delimited statements from a
   * <code>file</code> in a <code>directory</code>, making <code>hivevar</code>
   * substitutions from <code>parameters</code>
   *
   * @param directory
   * @param file
   * @param parameters
   * @return {@link List} of {@link List} of {@link String} results per
   *         statement, no result will be indicated by 1-length empty
   *         {@link String} {@link List}
   * @throws Exception
   */
  public static List<List<String>> processStatement(String directory, String file, Map<String, String> parameters)
      throws Exception {
    return processStatement(directory, file, parameters, Collections.<String, String> emptyMap());
  }

  /**
   * Process a set of <code>;</code> delimited statements from a
   * <code>file</code> in a <code>directory</code>, making <code>hivevar</code>
   * substitutions from <code>parameters</code> and session settings from
   * <code>configuration</code>
   *
   * @param directory
   * @param file
   * @param parameters
   * @param configuration
   * @return {@link List} of {@link List} of {@link String} results per
   *         statement, no result will be indicated by 1-length empty
   *         {@link String} {@link List}
   * @throws Exception
   */
  public static List<List<String>> processStatement(String directory, String file, Map<String, String> parameters,
      Map<String, String> configuration) throws Exception {
    return processStatement(directory, file, parameters, configuration, MAX_RESULTS_DEFAULT);
  }

  /**
   * Process a set of <code>;</code> delimited statements from a
   * <code>file</code> in a <code>directory</code>, making <code>hivevar</code>
   * substitutions from <code>parameters</code> and session settings from
   * <code>configuration</code>
   *
   * @param directory
   * @param file
   * @param parameters
   * @param configuration
   * @param maxResults
   * @return {@link List} of {@link List} of {@link String} results per
   *         statement, no result will be indicated by 1-length empty
   *         {@link String} {@link List}
   * @throws Exception
   */
  public static List<List<String>> processStatement(String directory, String file, Map<String, String> parameters,
      Map<String, String> configuration, int maxResults) throws Exception {
    List<List<String>> results = new ArrayList<List<String>>();
    for (String statement : readFileToLines(directory, file, COMMAND_DELIMETER)) {
      results.add(processStatement(statement, parameters, configuration, maxResults));
    }
    return results;
  }

  /**
   * Get a new Hive bootstrap configuration, can be overridden to provide custom
   * settings
   *
   * @return
   */
  public HiveConf getConfBootstrap() {
    return new HiveConf(new Configuration(), CopyTask.class);
  }

  @BeforeClass
  public static void setUpRuntime() throws Exception {
    long time = debugMessageHeader(LOG, "setUpRuntime");
    HiveConf confBootstrap = new MiniClusterDfsMrHiveTest().getConfBootstrap();
    miniHs2 = new MiniHS2(confBootstrap, true);
    Map<String, String> confOverlay = new HashMap<String, String>();
    confOverlay.put(ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    confOverlay.put(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
    miniHs2.start(confOverlay);
    fileSystem = miniHs2.getDfs().getFileSystem();
    SessionState.start(new SessionState(confBootstrap));
    conf = miniHs2.getHiveConf();
    debugMessageFooter(LOG, "setUpRuntime", time);
  }

  @Before
  public void setUpSchema() throws Exception {
    long time = debugMessageHeader(LOG, "setUpSchema");
    for (String table : processStatement("SHOW TABLES")) {
      if (table.length() > 0) {
        processStatement("DROP TABLE " + table);
      }
    }
    debugMessageFooter(LOG, "setUpSchema", time);
  }

  @AfterClass
  public static void tearDownRuntime() throws Exception {
    long time = debugMessageHeader(LOG, "tearDownRuntime");
    if (miniHs2 != null) {
      miniHs2.stop();
    }
    debugMessageFooter(LOG, "tearDownRuntime", time);
  }

  private static List<String> readFileToLines(String directory, String file, String delimeter) throws IOException {
    List<String> lines = new ArrayList<String>();
    InputStream inputStream = MiniClusterDfsMrHiveTest.class.getResourceAsStream(directory + "/" + file);
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
    throw new IOException("Could not load file [" + directory + "/" + file + "] from classpath");
  }

}
