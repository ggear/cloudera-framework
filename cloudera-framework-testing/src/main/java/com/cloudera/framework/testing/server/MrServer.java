package com.cloudera.framework.testing.server;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.shims.HadoopShims.MiniMrShim;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapreduce.MRConfig;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MR2 {@link TestRule}
 */
public class MrServer extends CdhServer<MrServer, MrServer.Runtime> {

  public enum Runtime {
    LOCAL_JOB, // Local MR2 job runner, inline-thread, light-weight
    CLUSTER_JOB // Mini MR2 cluster, multi-threaded, heavy-weight
  };

  /**
   * Get instance with default runtime
   *
   * @return
   */
  public static synchronized MrServer getInstance() {
    return getInstance(instance == null ? Runtime.LOCAL_JOB : instance.getRuntime());
  }

  /**
   * Get instance with specific <code>runtime</code>
   *
   * @return
   */
  public static synchronized MrServer getInstance(Runtime runtime) {
    return instance == null ? instance = new MrServer(runtime) : instance.assertRuntime(runtime);
  }

  @Override
  public int getIndex() {
    return 60;
  }

  @Override
  public CdhServer<?, ?>[] getDependencies() {
    return new CdhServer<?, ?>[] { DfsServer.getInstance() };
  }

  @Override
  public synchronized void start() throws Exception {
    long time = log(LOG, "start");
    switch (getRuntime()) {
    case LOCAL_JOB:
      getConf().set(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
      break;
    case CLUSTER_JOB:
      for (File file : new File(ABS_DIR_TARGET).listFiles(new FileFilter() {
        @Override
        public boolean accept(File pathname) {
          return pathname.isDirectory() && pathname.getName().startsWith(DIR_RUNTIME_MR);
        }
      })) {
        FileUtils.deleteDirectory(file);
      }
      miniMr = ShimLoader.getHadoopShims().getMiniMrCluster(getConf(), 1, DfsServer.getInstance().getFileSystem().getUri().toString(), 1);
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
    case LOCAL_JOB:
      break;
    case CLUSTER_JOB:
      if (miniMr != null) {
        miniMr.shutdown();
        miniMr = null;
      }
      break;
    default:
      throw new IllegalArgumentException("Unsupported [" + getClass().getSimpleName() + "] runtime [" + getRuntime() + "]");
    }
    log(LOG, "stop", time);
  }

  private static Logger LOG = LoggerFactory.getLogger(MrServer.class);

  private MiniMrShim miniMr;

  private static MrServer instance;

  private MrServer(Runtime runtime) {
    super(runtime);
  }

}
