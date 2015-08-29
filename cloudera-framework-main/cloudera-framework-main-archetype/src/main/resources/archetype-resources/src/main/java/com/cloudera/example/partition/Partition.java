package com.cloudera.example.partition;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormatNoCheck;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.example.Constants;
import com.cloudera.example.model.Record;
import com.cloudera.example.model.RecordCounter;
import com.cloudera.example.model.RecordFactory;
import com.cloudera.example.model.RecordKey;
import com.cloudera.example.model.RecordPartition;
import com.cloudera.framework.main.common.Driver;
import com.cloudera.framework.main.common.util.DfsUtil;

/**
 * Partition driver, take a set of staged sequence files and rewrite them into
 * consolidated, schema partitioned, row order Avro
 * {@link Record#getClassSchema() files}. Malformed and duplicate records are
 * annexed off and written in the source text file format with {@link RecordKey
 * key} and original {@link Text} value with originating source directory,
 * partition and file name.
 */
public class Partition extends Driver {

  public static final RecordCounter[] COUNTERS = new RecordCounter[] { RecordCounter.RECORDS,
      RecordCounter.RECORDS_CANONICAL, RecordCounter.RECORDS_DUPLICATE, RecordCounter.RECORDS_MALFORMED };

  protected static final String OUTPUT_TEXT = "text";
  protected static final String OUTPUT_AVRO = "avro";

  private static final Logger LOG = LoggerFactory.getLogger(Partition.class);

  private Path inputPath;
  private Path outputPath;
  private Set<Path> inputPaths;

  private FileSystem hdfs;

  public Partition() {
    super();
  }

  public Partition(Configuration confguration) {
    super(confguration);
  }

  @Override
  public String description() {
    return "Partition my dataset";
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
      incrementCounter(Partition.class.getCanonicalName(), counter, 0);
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
      job.setJarByClass(Partition.class);
      job.getConfiguration().set(Constants.CONFIG_INPUT_PATH, inputPath.toString());
      job.getConfiguration().set(Constants.CONFIG_OUTPUT_PATH, outputPath.toString());
      job.getConfiguration().set(FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, Boolean.FALSE.toString());
      for (Path inputPath : inputPaths) {
        MultipleInputs.addInputPath(job, inputPath, RecordFactory.getRecordSequenceInputFormat(
            RecordPartition.getPartitionPathName(inputPath, RecordPartition.BATCH_COL_ID_START_FINISH, 2)));
      }
      job.setMapperClass(Mapper.class);
      job.setSortComparatorClass(RecordKey.RecordKeyComparator.class);
      job.setMapOutputKeyClass(RecordKey.class);
      AvroJob.setMapOutputValueSchema(job, Record.getClassSchema());
      job.setReducerClass(Reducer.class);
      job.setNumReduceTasks(inputPaths.size());
      FileOutputFormat.setOutputPath(job, outputPath);
      LazyOutputFormatNoCheck.setOutputFormatClass(job, TextOutputFormat.class);
      MultipleOutputs.addNamedOutput(job, OUTPUT_TEXT, TextOutputFormat.class, RecordKey.class, Text.class);
      AvroMultipleOutputs.addNamedOutput(job, OUTPUT_AVRO, AvroKeyOutputFormat.class, Record.getClassSchema());
      jobSuccess = job.waitForCompletion(LOG.isInfoEnabled());
      for (Path path : inputPaths) {
        hdfs.createNewFile(new Path(path, FileOutputCommitter.SUCCEEDED_FILE_NAME));
      }
      importCounters(job, COUNTERS);
    }
    return jobSuccess ? RETURN_SUCCESS : RETURN_FAILURE_RUNTIME;
  }

  /**
   * Mapper.<br>
   * <br>
   * Note this class is not thread-safe but is jvm-reuse-safe, reusing objects
   * where possible.
   */
  private static class Mapper
      extends org.apache.hadoop.mapreduce.Mapper<RecordKey, AvroGenericRecordWritable, RecordKey, AvroValue<Record>> {

    private final RecordKey recordKey = new RecordKey();
    private final Record recordValue = new Record();
    private final Text textValue = new Text();
    private final StringBuilder string = new StringBuilder(512);
    private final AvroValue<Record> recordWrapped = new AvroValue<Record>();
    private final String MALFORMED_PATH_PREFIX = Constants.DIR_REL_MYDS_MALFORMED + Path.SEPARATOR_CHAR + OUTPUT_TEXT
        + Path.SEPARATOR_CHAR;

    private MultipleOutputs<RecordKey, Text> multipleOutputs;

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected void setup(
        org.apache.hadoop.mapreduce.Mapper<RecordKey, AvroGenericRecordWritable, RecordKey, AvroValue<Record>>.Context context)
            throws IOException, InterruptedException {
      multipleOutputs = new MultipleOutputs(context);
    }

    @Override
    protected void cleanup(
        org.apache.hadoop.mapreduce.Mapper<RecordKey, AvroGenericRecordWritable, RecordKey, AvroValue<Record>>.Context context)
            throws IOException, InterruptedException {
      multipleOutputs.close();
    }

    @Override
    protected void map(RecordKey key, AvroGenericRecordWritable value,
        org.apache.hadoop.mapreduce.Mapper<RecordKey, AvroGenericRecordWritable, RecordKey, AvroValue<Record>>.Context context)
            throws IOException, InterruptedException {
      if (key.isValid()) {
        key.setHash(recordValue.hashCode());
        recordWrapped.datum((Record) value.getRecord());
        context.write(recordKey, recordWrapped);
      } else {
        context.getCounter(RecordCounter.RECORDS).increment(1);
        context.getCounter(RecordCounter.RECORDS_MALFORMED).increment(1);
        textValue.set(key.getSource());
        string.setLength(0);
        multipleOutputs.write(OUTPUT_TEXT, NullWritable.get(), textValue,
            string.append(MALFORMED_PATH_PREFIX).append(key.getBatch()).toString());
      }
    }

  }

  /**
   * Reducer.<br>
   * <br>
   * Note this class is not thread-safe but is jvm-reuse-safe, reusing objects
   * where possible.
   */
  private static class Reducer
      extends org.apache.hadoop.mapreduce.Reducer<RecordKey, AvroValue<Record>, NullWritable, AvroValue<Record>> {

    private static final String PARTITION_YEAR = RecordPartition.RECORD_COL_YEAR_MONTH[0] + "=";
    private static final String PARTITION_MONTH = RecordPartition.RECORD_COL_YEAR_MONTH[1] + "=";
    private static final String PARTITION_FILE = "mydataset-" + UUID.randomUUID().toString();

    private final AvroKey<Record> record = new AvroKey<Record>();
    private final StringBuilder string = new StringBuilder(512);
    private final Set<String> partitions = new HashSet<String>(50);
    private final Set<AvroValue<Record>> records = new HashSet<AvroValue<Record>>(32);
    private final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

    private AvroMultipleOutputs multipleOutputsAvro;

    @Override
    protected void setup(
        org.apache.hadoop.mapreduce.Reducer<RecordKey, AvroValue<Record>, NullWritable, AvroValue<Record>>.Context context)
            throws IOException, InterruptedException {
      multipleOutputsAvro = new AvroMultipleOutputs(context);
      partitions.clear();
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      for (String partition : partitions) {
        FileSystem.get(context.getConfiguration())
            .delete(new Path(context.getConfiguration().get(FileOutputFormat.OUTDIR) + Path.SEPARATOR_CHAR + partition,
                FileOutputCommitter.SUCCEEDED_FILE_NAME), false);
      }
      partitions.clear();
      multipleOutputsAvro.close();
    }

    @Override
    protected void reduce(RecordKey key, Iterable<AvroValue<Record>> values,
        org.apache.hadoop.mapreduce.Reducer<RecordKey, AvroValue<Record>, NullWritable, AvroValue<Record>>.Context context)
            throws IOException, InterruptedException {
      records.clear();
      Iterator<AvroValue<Record>> valuesIterator = values.iterator();
      while (valuesIterator.hasNext()) {
        AvroValue<Record> record = valuesIterator.next();
        context.getCounter(RecordCounter.RECORDS).increment(1);
        RecordCounter counter = records.add(record) ? RecordCounter.RECORDS_CANONICAL : RecordCounter.RECORDS_DUPLICATE;
        context.getCounter(counter).increment(1);
        this.record.datum(record.datum());
        calendar.setTimeInMillis(record.datum().getMyTimestamp());
        string.setLength(0);
        string
            .append(counter.equals(RecordCounter.RECORDS_CANONICAL) ? Constants.DIR_REL_MYDS_PARTITIONED_CANONICAL_AVRO
                : Constants.DIR_REL_MYDS_PARTITIONED_DUPLICATE_AVRO)
            .append(Path.SEPARATOR_CHAR).append(PARTITION_YEAR).append(calendar.get(Calendar.YEAR))
            .append(Path.SEPARATOR_CHAR).append(PARTITION_MONTH).append(calendar.get(Calendar.MONTH) + 1)
            .append(Path.SEPARATOR_CHAR);
        partitions.add(string.toString());
        multipleOutputsAvro.write(OUTPUT_AVRO, this.record, NullWritable.get(),
            string.append(PARTITION_FILE).toString());
      }
    }

  }

  public static void main(String... arguments) throws Exception {
    System.exit(new Partition().runner(arguments));
  }

}
