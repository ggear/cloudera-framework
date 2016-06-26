package com.cloudera.framework.testing.server;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Python {@link TestRule}
 */
public class PythonServer extends CdhServer<PythonServer, PythonServer.Runtime> {

  public enum Runtime {
    LOCAL_CPYTHON
  };

  public static synchronized PythonServer getInstance() {
    return getInstance(instance == null ? Runtime.LOCAL_CPYTHON : instance.getRuntime());
  }

  public static synchronized PythonServer getInstance(Runtime runtime) {
    return instance == null ? instance = new PythonServer(runtime) : instance.assertRuntime(runtime);
  }

  /**
   * Process a script from <code>file</code> sourced from
   * <code>directory</code>, invoking with command line <code>parameters</code>
   *
   * @param directory
   * @param file
   * @return the exit code
   * @throws InterruptedException
   * @throws Exception
   */
  public int execute(String directory, String file) throws IOException, InterruptedException {
    return execute(directory, file, null);
  }

  /**
   * Process a script from <code>file</code> sourced from
   * <code>directory</code>, invoking with command line <code>parameters</code>
   *
   * @param directory
   * @param file
   * @param parameters
   * @return the exit code
   * @throws InterruptedException
   * @throws Exception
   */
  public int execute(String directory, String file, List<String> parameters) throws IOException, InterruptedException {
    return execute(directory, file, parameters, null);
  }

  /**
   * Process a script from <code>file</code> sourced from
   * <code>directory</code>, invoking with command line <code>parameters</code>
   *
   * @param directory
   * @param file
   * @param parameters
   * @param configuration
   * @return the exit code
   * @throws InterruptedException
   * @throws Exception
   */
  public int execute(String directory, String file, List<String> parameters, Map<String, String> configuration)
      throws IOException, InterruptedException {
    return execute(directory, file, parameters, configuration, null, null);
  }

  /**
   * Process a script from <code>file</code> sourced from
   * <code>directory</code>, invoking with command line <code>parameters</code>
   *
   * @param directory
   * @param file
   * @param parameters
   * @param configuration
   * @param output
   * @param error
   * @return the exit code
   * @throws InterruptedException
   * @throws Exception
   */
  public int execute(String directory, String file, List<String> parameters, Map<String, String> configuration, StringBuffer output,
      StringBuffer error) throws IOException, InterruptedException {
    return execute(directory, file, parameters, configuration, output, error, false);
  }

  /**
   * Process a script from <code>file</code> sourced from
   * <code>directory</code>, invoking with command line <code>parameters</code>
   *
   * @param directory
   * @param file
   * @param parameters
   * @param configuration
   * @param output
   * @param error
   * @param quiet
   * @return the exit code
   * @throws InterruptedException
   * @throws Exception
   */
  public int execute(String directory, String file, List<String> parameters, Map<String, String> configuration, StringBuffer output,
      StringBuffer error, boolean quiet) throws IOException, InterruptedException {
    int exit = -1;
    URL directoryUrl = PythonServer.class.getResource(StringUtils.isEmpty(directory) ? "/" : directory);
    File script = new File(directoryUrl == null ? directory : directoryUrl.getFile(), StringUtils.isEmpty(file) ? "" : file);
    if (!script.exists()) {
      throw new IOException("Could not find file [" + script.getAbsolutePath() + "]");
    }
    if (!script.canExecute()) {
      script.setExecutable(true);
    }
    List<String> command = new ArrayList<>(parameters == null ? Collections.<String> emptyList() : parameters);
    command.add(0, script.getAbsolutePath());
    Process process = new ProcessBuilder(command).start();
    IOUtils.closeQuietly(process.getOutputStream());
    exit = process.waitFor();
    String inputString = StringUtils.removeEnd(IOUtils.toString(process.getInputStream()), "\n");
    IOUtils.closeQuietly(process.getInputStream());
    if (output != null) {
      output.append(inputString);
    }
    String errorString = StringUtils.removeEnd(IOUtils.toString(process.getErrorStream()), "\n");
    IOUtils.closeQuietly(process.getErrorStream());
    if (error != null) {
      error.append(errorString);
    }
    if (!quiet) {
      log(LOG, "execute",
          "script [" + script.getAbsolutePath() + "]\n" + inputString + (StringUtils.isEmpty(errorString) ? "" : errorString), true);
    }
    return exit;
  }

  @Override
  public int getIndex() {
    return 90;
  }

  @Override
  public CdhServer<?, ?>[] getDependencies() {
    return new CdhServer<?, ?>[0];
  }

  @Override
  public synchronized void start() throws Exception {
    long time = log(LOG, "start");
    switch (getRuntime()) {
    case LOCAL_CPYTHON:
      break;
    default:
      throw new IllegalArgumentException("Unsupported [" + getClass().getSimpleName() + "] runtime [" + getRuntime() + "]");
    }
    log(LOG, "start", time);
  }

  @Override
  public synchronized void stop() throws IOException {
    long time = log(LOG, "stop");
    switch (getRuntime()) {
    case LOCAL_CPYTHON:
      break;
    default:
      throw new IllegalArgumentException("Unsupported [" + getClass().getSimpleName() + "] runtime [" + getRuntime() + "]");
    }
    log(LOG, "stop", time);
  }

  private static Logger LOG = LoggerFactory.getLogger(PythonServer.class);

  private static PythonServer instance;

  private PythonServer(Runtime runtime) {
    super(runtime);
  }

}
