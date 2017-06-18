package com.cloudera.framework.testing.server;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManagerImpl;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.Service;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.thrift.ThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.apache.hive.service.cli.thrift.ThriftHttpCLIService;
import org.apache.hive.service.server.HiveServer2;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hive {@link TestRule}
 */
public class HiveServer extends CdhServer<HiveServer, HiveServer.Runtime> {

  public static final String HS2_BINARY_MODE = "binary";

  public static final String HS2_HTTP_MODE = "http";
  private static final String DIR_HOME = "usr/hive";
  private static final String DIR_SCRATCH = "scratch";
  private static final String DIR_WAREHOUSE = "warehouse";
  private static final String COMMAND_DELIMETER = ";";
  private static final int MAX_RESULTS_DEFAULT = 100;
  private static final AtomicLong DERBY_DB_COUNTER = new AtomicLong();
  private static final String HIVE_CONF_SPARK_MASTER = "spark.master";

  private static final Logger LOG = LoggerFactory.getLogger(HiveServer.class);

  private static HiveServer instance;

  private final int httpPort;
  private final int binaryPort;
  private HiveServer2 hiveServer;

  private HiveServer(Runtime runtime) {
    super(runtime);
    httpPort = CdhServer.getNextAvailablePort();
    binaryPort = CdhServer.getNextAvailablePort();
  }

  /**
   * Get instance with default runtime
   */
  public static synchronized HiveServer getInstance() {
    return getInstance(instance == null ? Runtime.LOCAL_MR2 : instance.getRuntime());
  }

  /**
   * Get instance with specific <code>runtime</code>
   */
  public static synchronized HiveServer getInstance(Runtime runtime) {
    return instance == null ? instance = new HiveServer(runtime) : instance.assertRuntime(runtime);
  }

  /**
   * Process a <code>statement</code>
   *
   * @return {@link List} of {@link String} results, no result will be indicated
   * by 1-length empty {@link String} {@link List}
   */
  public List<String> execute(String statement) throws Exception {
    return execute(statement, Collections.emptyMap(), Collections.emptyMap());
  }

  /**
   * Process a <code>statement</code>, making <code>hivevar</code> substitutions
   * from <code>parameters</code>
   *
   * @return {@link List} of {@link String} results, no result will be indicated
   * by 1-length empty {@link String} {@link List}
   */
  public List<String> execute(String statement, Map<String, String> parameters) throws Exception {
    return execute(statement, parameters, Collections.emptyMap());
  }

  /**
   * Process a <code>statement</code>, making <code>hivevar</code> substitutions
   * from <code>parameters</code> and session settings from
   * <code>configuration</code>
   *
   * @return {@link List} of {@link String} results, no result will be indicated
   * by 1-length empty {@link String} {@link List}
   */
  public List<String> execute(String statement, Map<String, String> parameters, Map<String, String> configuration) throws Exception {
    return execute(statement, parameters, configuration, MAX_RESULTS_DEFAULT);
  }

  /**
   * Process a <code>statement</code>, making <code>hivevar</code> substitutions
   * from <code>parameters</code> and session settings from
   * <code>configuration</code>
   *
   * @return {@link List} of {@link String} results, no result will be indicated
   * by 1-length empty {@link String} {@link List}
   */
  public List<String> execute(String statement, Map<String, String> parameters, Map<String, String> configuration, int maxResults)
    throws Exception {
    return execute(statement, parameters, configuration, maxResults, false);
  }

  /**
   * Process a <code>statement</code>, making <code>hivevar</code> substitutions
   * from <code>parameters</code> and session settings from
   * <code>configuration</code>
   *
   * @return {@link List} of {@link String} results, no result will be indicated
   * by 1-length empty {@link String} {@link List}
   */
  public List<String> execute(String statement, Map<String, String> parameters, Map<String, String> configuration, int maxResults,
                              boolean quiet) throws Exception {
    long time = System.currentTimeMillis();
    if (!quiet) {
      log(LOG, "execute", true);
    }
    HiveConf confSession = new HiveConf((HiveConf) getConf());
    confSession.set(HiveConf.ConfVars.HIVEQUERYID.varname, QueryPlan.makeQueryId());
    for (String key : configuration.keySet()) {
      confSession.set(key, configuration.get(key));
    }
    List<String> results = new ArrayList<>();
    CommandProcessor commandProcessor = CommandProcessorFactory.getForHiveCommand(
      (statement = new StrSubstitutor(parameters, "${hivevar:", "}").replace(statement.trim())).split("\\s+"), confSession);
    if (commandProcessor == null) {
      ((Driver) (commandProcessor = new Driver(confSession))).setMaxRows(maxResults);
    }
    if (!quiet) {
      log(LOG, "execute", "statement:\n" + statement, true);
    }
    String responseErrorMessage = null;
    int responseCode = commandProcessor.run(statement).getResponseCode();
    if (commandProcessor instanceof Driver) {
      ((Driver) commandProcessor).getResults(results);
      responseErrorMessage = ((Driver) commandProcessor).getErrorMsg();
    }
    if (!quiet) {
      if (responseCode != 0 || responseErrorMessage != null) {
        log(LOG, "execute",
          "error code [" + responseCode + "]" + (responseErrorMessage != null ? " message [" + responseErrorMessage + " ]" : ""), true);
      } else {
        log(LOG, "execute", "results count [" + results.size() + (results.size() == maxResults ? " (MAX)" : "") + "]:\n"
          + StringUtils.join(results.toArray(), "\n"), true);
      }
      log(LOG, "execute", "finished in [" + (System.currentTimeMillis() - time) + "] ms", true);
    }
    if (responseCode != 0 || responseErrorMessage != null) {
      throw new SQLException("Statement executed with error response code [" + responseCode + "]"
        + (responseErrorMessage != null ? " and error message [" + responseErrorMessage + " ]" : ""));
    }
    return results;
  }

  /**
   * Process a set of <code>;</code> delimited statements from a
   * <code>file</code>
   *
   * @return {@link List} of {@link List} of {@link String} results per
   * statement, no result will be indicated by 1-length empty
   * {@link String} {@link List}
   */
  public List<List<String>> execute(File file) throws Exception {
    return execute(file, Collections.emptyMap(), Collections.emptyMap());
  }

  /**
   * Process a set of <code>;</code> delimited statements from a
   * <code>file</code>, making <code>hivevar</code> substitutions from
   * <code>parameters</code>
   *
   * @return {@link List} of {@link List} of {@link String} results per
   * statement, no result will be indicated by 1-length empty
   * {@link String} {@link List}
   */
  public List<List<String>> execute(File file, Map<String, String> parameters) throws Exception {
    return execute(file, parameters, Collections.emptyMap());
  }

  /**
   * Process a set of <code>;</code> delimited statements from a
   * <code>file</code>, making <code>hivevar</code> substitutions from
   * <code>parameters</code> and session settings from
   * <code>configuration</code>
   *
   * @return {@link List} of {@link List} of {@link String} results per
   * statement, no result will be indicated by 1-length empty
   * {@link String} {@link List}
   */
  public List<List<String>> execute(File file, Map<String, String> parameters, Map<String, String> configuration) throws Exception {
    return execute(file, parameters, configuration, MAX_RESULTS_DEFAULT);
  }

  /**
   * Process a set of <code>;</code> delimited statements from a
   * <code>file</code>, making <code>hivevar</code> substitutions from
   * <code>parameters</code> and session settings from
   * <code>configuration</code>
   *
   * @return {@link List} of {@link List} of {@link String} results per
   * statement, no result will be indicated by 1-length empty
   * {@link String} {@link List}
   */
  public List<List<String>> execute(File file, Map<String, String> parameters, Map<String, String> configuration, int maxResults)
    throws Exception {
    List<List<String>> results = new ArrayList<>();
    if (file == null) {
      throw new IOException("File [null] not found");
    }
    log(LOG, "execute", "script [" + file.getAbsolutePath() + "]");
    for (String statement : FileUtils.readFileToString(file).split(COMMAND_DELIMETER)) {
      if (!StringUtils.isEmpty(statement.trim())) {
        results.add(execute(statement, parameters, configuration, maxResults));
      }
    }
    return results;
  }

  @Override
  public int getIndex() {
    return 80;
  }

  @Override
  public CdhServer<?, ?>[] getDependencies() {
    return new CdhServer<?, ?>[]{DfsServer.getInstance(), MrServer.getInstance()};
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Override
  public synchronized void start() throws Exception {
    long time = log(LOG, "start");
    Path hiveHomePath = new Path(DfsServer.getInstance().getPathUri("/"), DIR_HOME);
    Path hiveWarehousePath = new Path(hiveHomePath, DIR_WAREHOUSE);
    Path hiveScratchPath = new Path(hiveHomePath, DIR_SCRATCH);
    File hiveScratchLocalPath = new File(ABS_DIR_HIVE, DIR_SCRATCH);
    File derbyDir = new File(ABS_DIR_DERBY_DB);
    String hiveDerbyConnectString = "jdbc:derby:" + derbyDir.getAbsolutePath() + "/test-hive-metastore-"
      + DERBY_DB_COUNTER.incrementAndGet() + ";create=true";
    FileUtils.deleteDirectory(derbyDir);
    derbyDir.mkdirs();
    DfsServer.getInstance().getFileSystem().mkdirs(hiveHomePath);
    DfsServer.getInstance().getFileSystem().mkdirs(hiveWarehousePath);
    FileSystem.mkdirs(DfsServer.getInstance().getFileSystem(), hiveWarehousePath, new FsPermission((short) 511));
    FileSystem.mkdirs(DfsServer.getInstance().getFileSystem(), hiveScratchPath, new FsPermission((short) 475));
    HiveConf hiveConf = new HiveConf(HiveServer.class);
    hiveConf.setVar(ConfVars.METASTOREWAREHOUSE, hiveWarehousePath.toString());
    hiveConf.setVar(ConfVars.METASTORECONNECTURLKEY, hiveDerbyConnectString);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, "localhost");
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT, binaryPort);
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT, httpPort);
    hiveConf.setVar(ConfVars.SCRATCHDIR, hiveScratchPath.toString());
    hiveConf.setVar(ConfVars.LOCALSCRATCHDIR, hiveScratchLocalPath.getAbsolutePath());
    hiveConf.set(CommonConfigurationKeysPublic.HADOOP_SHELL_MISSING_DEFAULT_FS_WARNING_KEY, "false");
    hiveConf.setVar(ConfVars.HIVEINPUTFORMAT, CombineHiveInputFormat.class.getName());
    hiveConf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, Boolean.FALSE);
    hiveConf.setBoolVar(ConfVars.LOCALMODEAUTO, Boolean.FALSE);
    hiveConf.setBoolVar(ConfVars.HIVECONVERTJOIN, Boolean.FALSE);
    hiveConf.setBoolVar(ConfVars.HIVEIGNOREMAPJOINHINT, Boolean.FALSE);
    hiveConf.set(SparkServer.SPARK_CONF_MULTI_CONTEXTS, "true");
    switch (getRuntime()) {
      case LOCAL_MR2:
        break;
      case LOCAL_SPARK:
        hiveConf.setVar(ConfVars.HIVE_EXECUTION_ENGINE, "spark");
        hiveConf.set(HIVE_CONF_SPARK_MASTER, "local");
        break;
      default:
        throw new IllegalArgumentException("Unsupported [" + getClass().getSimpleName() + "] runtime [" + getRuntime() + "]");
    }
    hiveServer = new HiveServer2();
    hiveServer.init(hiveConf);
    hiveServer.start();
    waitForStart();
    SessionState.start(new SessionState(hiveConf));
    setConf(hiveConf);
    log(LOG, "start", time);
  }

  @Override
  public synchronized void clean() throws Exception {
    long time = log(LOG, "clean");
    for (String table : execute("SHOW TABLES", Collections.emptyMap(), Collections.emptyMap(),
      MAX_RESULTS_DEFAULT, true)) {
      if (table.length() > 0) {
        execute("DROP TABLE " + table, Collections.emptyMap(), Collections.emptyMap(),
          MAX_RESULTS_DEFAULT, true);
      }
    }
    for (String database : execute("SHOW DATABASES", Collections.emptyMap(), Collections.emptyMap(),
      MAX_RESULTS_DEFAULT, true)) {
      if (database.length() > 0 && !database.equals("default")) {
        execute("DROP DATABASE " + database + " CASCADE", Collections.emptyMap(), Collections.emptyMap(),
          MAX_RESULTS_DEFAULT, true);
      }
    }
    log(LOG, "clean", time);
  }

  @Override
  public synchronized void state() throws Exception {
    long time = log(LOG, "state", true);
    log(LOG, "state", "tables:\n" + StringUtils.join(
      execute("SHOW TABLES", Collections.emptyMap(), Collections.emptyMap(), MAX_RESULTS_DEFAULT, true)
        .toArray(),
      "\n"), true);
    log(LOG, "state", time, true);
  }

  @Override
  public synchronized void stop() throws IOException, HiveException {
    long time = log(LOG, "stop");
    switch (getRuntime()) {
      case LOCAL_MR2:
        break;
      case LOCAL_SPARK:
        SparkSessionManagerImpl.getInstance().shutdown();
        break;
      default:
        throw new IllegalArgumentException("Unsupported [" + getClass().getSimpleName() + "] runtime [" + getRuntime() + "]");
    }
    if (hiveServer != null) {
      hiveServer.stop();
    }
    log(LOG, "stop", time);
  }

  private String getJdbcURL() {
    String dbName = "default";
    String sessionConfExt = "";
    String hiveConfExt = "";
    if (isHttpTransportMode()) {
      hiveConfExt = "hive.server2.transport.mode=http;hive.server2.thrift.http.path=cliservice;" + hiveConfExt;
    }
    if (!hiveConfExt.trim().equals("")) {
      hiveConfExt = "?" + hiveConfExt;
    }
    return "jdbc:hive2://localhost" + ":" + (isHttpTransportMode() ? httpPort : binaryPort) + "/" + dbName + sessionConfExt + hiveConfExt;
  }

  private CLIServiceClient getClient() {
    for (Service service : hiveServer.getServices()) {
      if (service instanceof ThriftBinaryCLIService) {
        return new ThriftCLIServiceClient((ThriftBinaryCLIService) service);
      }
      if (service instanceof ThriftHttpCLIService) {
        return new ThriftCLIServiceClient((ThriftHttpCLIService) service);
      }
    }
    throw new IllegalStateException("HiveServer2 not running Thrift service");
  }

  private boolean isHttpTransportMode() {
    String transportMode = getConf().get(ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname);
    return transportMode != null && transportMode.equalsIgnoreCase(HS2_HTTP_MODE);
  }

  private void waitForStart() throws InterruptedException, TimeoutException, HiveSQLException {
    int waitTime = 0;
    long pollPeriod = 100L;
    long startupTimeout = 1000L * 1000L;
    CLIServiceClient hiveClient = getClient();
    SessionHandle sessionHandle;
    do {
      Thread.sleep(pollPeriod);
      waitTime += pollPeriod;
      if (waitTime > startupTimeout) {
        throw new TimeoutException("Couldn't access new HiveServer2: " + getJdbcURL());
      }
      try {
        Map<String, String> sessionConf = new HashMap<>();
        sessionHandle = hiveClient.openSession("foo", "bar", sessionConf);
      } catch (Exception e) {
        continue;
      }
      hiveClient.closeSession(sessionHandle);
      break;
    } while (true);
  }

  public enum Runtime {
    LOCAL_MR2, // Local MR2 job runner backed Hive, inline-thread, light-weight
    LOCAL_SPARK // Spark local backed Hive, single-threaded, light-weight
  }

}
