package com.cloudera.framework.testing.server;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Impala {@link TestRule}
 */
public class ImpalaServer extends CdhServer<ImpalaServer, ImpalaServer.Runtime> {

  // TODO: Provide implementation

  private static final int MAX_RESULTS_DEFAULT = 100;

  private static final Logger LOG = LoggerFactory.getLogger(ImpalaServer.class);

  private static ImpalaServer instance;

  private ImpalaServer(Runtime runtime) {
    super(runtime);
  }

  /**
   * Get instance with default runtime
   */
  public static synchronized ImpalaServer getInstance() {
    return getInstance(instance == null ? Runtime.CLUSTER_DEAMONS : instance.getRuntime());
  }

  /**
   * Get instance with specific <code>runtime</code>
   */
  public static synchronized ImpalaServer getInstance(Runtime runtime) {
    return instance == null ? instance = new ImpalaServer(runtime) : instance.assertRuntime(runtime);
  }

  /**
   * Rollup the counts of the results
   */
  public static List<Integer> count(List<List<String>> results) {
    List<Integer> counts = new ArrayList<>();
    results.forEach(result -> counts.add(result.size()));
    return counts;
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
    List<String> results = new ArrayList<>();
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
    return results;
  }

  @Override
  public int getIndex() {
    return 90;
  }

  @Override
  public CdhServer<?, ?>[] getDependencies() {
    return new CdhServer<?, ?>[]{DfsServer.getInstance()};
  }

  @Override
  public synchronized boolean testValidity() {
    if (!envOsName.equals("Linux")) {
      log(LOG, "error", "Linux required, " + envOsName + " detected");
      return false;
    }
    return true;
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Override
  public synchronized void start() throws Exception {
    long time = log(LOG, "start");
    log(LOG, "start", time);
  }

  @Override
  public synchronized void clean() throws Exception {
    long time = log(LOG, "clean");
    log(LOG, "clean", time);
  }

  @Override
  public synchronized void state() throws Exception {
    long time = log(LOG, "state", true);
    log(LOG, "state", time, true);
  }

  @Override
  public synchronized void stop() throws IOException, HiveException {
    long time = log(LOG, "stop");
    log(LOG, "stop", time);
  }

  private String getJdbcURL() {
    return "";
  }

  public enum Runtime {
    CLUSTER_DEAMONS // Mini Impala cluster, multi-process, heavy-weight
  }

}
