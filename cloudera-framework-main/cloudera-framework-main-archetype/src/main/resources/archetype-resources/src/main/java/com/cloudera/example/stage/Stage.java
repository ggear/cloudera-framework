package com.cloudera.example.stage;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormatNoCheck;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.example.Constants;
import com.cloudera.example.model.RecordCounter;
import com.cloudera.example.model.RecordKey;
import com.cloudera.example.model.input.RecordTextInputFormat;
import com.cloudera.framework.main.common.Driver;
import com.cloudera.framework.main.common.util.DfsUtil;

/**
 * Stage driver, take a set of UTF8 text files with a known naming scheme and
 * stage their records into a consolidated ingest-timestamp partitioned staging
 * set, written as file order, sequence files with {@link RecordKey keys} and
 * original {@link Text} values. Files not meeting the naming scheme are annexed
 * off and written in text format with source directory and file names.
 */
public class Stage extends Driver {

  public static final RecordCounter[] COUNTERS = new RecordCounter[] { RecordCounter.FILES,
      RecordCounter.FILES_CANONICAL, RecordCounter.FILES_DUPLICATE, RecordCounter.FILES_MALFORMED };

  protected static final String OUTPUT_TEXT = "text";
  protected static final String OUTPUT_SEQUENCE = "sequence";

  private static final Logger LOG = LoggerFactory.getLogger(Stage.class);

  private Path inputPath;
  private Path outputPath;
  private Set<Path> inputPaths;

  private FileSystem hdfs;

  public Stage() {
    super();
  }

  public Stage(Configuration confguration) {
    super(confguration);
  }

  @Override
  public String description() {
    return "Stage my dataset";
  }

  @Override
  public String[] options() {
    return new String[] {};
  }

  @Override
  public String[] parameters() {
    return new String[] { "input-path", "output-path" };
  }

  @Override
  public void reset() {
    super.reset();
    for (RecordCounter counter : COUNTERS) {
      incrementCounter(Stage.class.getCanonicalName(), counter, 0);
    }
  }

  @Override
  public int prepare(String... arguments) throws Exception {
    if (arguments == null || arguments.length != 2) {
      throw new Exception("Invalid number of arguments");
    }
    hdfs = FileSystem.newInstance(getConf());
    inputPath = hdfs.makeQualified(new Path(arguments[0]));
    if (LOG.isInfoEnabled()) {
      LOG.info("Input path [" + inputPath + "] validated");
    }
    inputPaths = DfsUtil.listDirs(hdfs, inputPath, true, true);
    outputPath = hdfs.makeQualified(new Path(arguments[1]));
    hdfs.mkdirs(outputPath.getParent());
    if (LOG.isInfoEnabled()) {
      LOG.info("Output path [" + outputPath + "] validated");
    }
    return RETURN_SUCCESS;
  }

  @Override
  public int execute() throws Exception {
    boolean jobSuccess = true;
    if (inputPaths.size() > 0) {
      Job job = Job.getInstance(getConf());
      job.setJobName(getClass().getSimpleName());
      job.setJarByClass(Stage.class);
      job.getConfiguration().set(Constants.CONFIG_INPUT_PATH, inputPath.toString());
      job.getConfiguration().set(Constants.CONFIG_OUTPUT_PATH, outputPath.toString());
      job.getConfiguration().set(FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, Boolean.FALSE.toString());
      job.setInputFormatClass(RecordTextInputFormat.class);
      for (Path inputPath : inputPaths) {
        FileInputFormat.addInputPath(job, inputPath);
      }
      job.setMapperClass(Mapper.class);
      job.setMapOutputKeyClass(RecordKey.class);
      job.setMapOutputValueClass(Text.class);
      job.setNumReduceTasks(0);
      FileOutputFormat.setOutputPath(job, outputPath);
      LazyOutputFormatNoCheck.setOutputFormatClass(job, TextOutputFormat.class);
      MultipleOutputs.addNamedOutput(job, OUTPUT_TEXT, TextOutputFormat.class, RecordKey.class, Text.class);
      MultipleOutputs.addNamedOutput(job, OUTPUT_SEQUENCE, SequenceFileOutputFormat.class, RecordKey.class, Text.class);
      jobSuccess = job.waitForCompletion(LOG.isInfoEnabled());
      for (Path path : inputPaths) {
        hdfs.createNewFile(new Path(path, FileOutputCommitter.SUCCEEDED_FILE_NAME));
      }
      importCounters(job, COUNTERS);
    }
    return jobSuccess ? RETURN_SUCCESS : RETURN_FAILURE_RUNTIME;
  }

  /**
   * Mapper, write partitions of data as consolidated sequence files keyed by
   * {@link RecordKey RecordKeys} and {@link Text} values. Malformed data is
   * stored in original text format, named as per their source.<br>
   * <br>
   * Note this class is not thread-safe but is jvm-reuse-safe, reusing objects
   * where possible.
   */
  private static class Mapper extends org.apache.hadoop.mapreduce.Mapper<RecordKey, Text, RecordKey, Text> {

    private final String PARTITION_BATCH_START = Path.SEPARATOR_CHAR + "ingest_batch_id=" + UUID.randomUUID()
        + Path.SEPARATOR_CHAR + "ingest_batch_start=";
    private final String PARTITION_FINISH = Path.SEPARATOR_CHAR + "ingest_batch_finish=";
    private final String PARTITION_PATH_SUFFIX = Path.SEPARATOR_CHAR + "mydataset";
    private final String PARTITION_PATH_PREFIX = Constants.DIR_DS_MYDATASET_CANONICAL + Path.SEPARATOR_CHAR
        + OUTPUT_SEQUENCE + Path.SEPARATOR_CHAR;
    private final String MALFORMED_PATH_PREFIX = Constants.DIR_DS_MYDATASET_MALFORMED + Path.SEPARATOR_CHAR;

    private final String timestamp = "" + System.currentTimeMillis();
    private final StringBuilder string = new StringBuilder(512);

    private MultipleOutputs<RecordKey, Text> multipleOutputs;

    @Override
    protected void setup(org.apache.hadoop.mapreduce.Mapper<RecordKey, Text, RecordKey, Text>.Context context)
        throws IOException, InterruptedException {
      multipleOutputs = new MultipleOutputs<RecordKey, Text>(context);
    }

    @Override
    protected void cleanup(org.apache.hadoop.mapreduce.Mapper<RecordKey, Text, RecordKey, Text>.Context context)
        throws IOException, InterruptedException {
      multipleOutputs.close();
    }

    @Override
    protected void map(RecordKey key, Text value,
        org.apache.hadoop.mapreduce.Mapper<RecordKey, Text, RecordKey, Text>.Context context)
            throws IOException, InterruptedException {
      string.setLength(0);
      context.getCounter(RecordCounter.FILES).increment(1);
      if (key.isValid()) {
        context.getCounter(RecordCounter.FILES_CANONICAL).increment(1);
        multipleOutputs.write(OUTPUT_SEQUENCE, key, value,
            string.append(PARTITION_PATH_PREFIX).append(key.getType()).append(Path.SEPARATOR_CHAR)
                .append(key.getCodec()).append(PARTITION_BATCH_START).append(timestamp).append(PARTITION_FINISH)
                .append(timestamp).append(PARTITION_PATH_SUFFIX).toString());
      } else {
        context.getCounter(RecordCounter.FILES_MALFORMED).increment(1);
        multipleOutputs.write(OUTPUT_TEXT, NullWritable.get(), value,
            string.append(MALFORMED_PATH_PREFIX).append(key.getBatch()).toString());
      }
    }

  }

  public static void main(String... arguments) throws Exception {
    System.exit(new Stage().runner(arguments));
  }

}
