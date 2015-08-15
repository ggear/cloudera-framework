package com.cloudera.example.process;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormatNoCheck;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.example.Constants;
import com.cloudera.example.model.Record;
import com.cloudera.example.model.RecordCounter;
import com.cloudera.example.model.RecordFactory;
import com.cloudera.example.model.RecordKey;
import com.cloudera.framework.main.common.Driver;
import com.cloudera.framework.main.common.util.DfsUtil;

/**
 * Process driver, take a set of staged sequence files and rewrite them into
 * consolidated, schema partitioned, row order Avro
 * {@link Record#getClassSchema() files}. The driver can be configured as a
 * pass-through, de-depulication and most-recent filter. Malformed files are
 * annexed off and written in the staging sequence file format with
 * {@link RecordKey key} and original String value.
 */
public class Process extends Driver {

  public static final RecordCounter[] COUNTERS = new RecordCounter[] { RecordCounter.RECORDS,
      RecordCounter.RECORDS_CLEANSED, RecordCounter.RECORDS_DUPLICATE, RecordCounter.RECORDS_MALFORMED };

  protected static final String OUTPUT_AVRO = "avro";
  protected static final String OUTPUT_SEQUENCE = "sequence";

  private static final Logger LOG = LoggerFactory.getLogger(Process.class);

  private Path inputPath;
  private Path outputPath;
  private Set<Path> inputPaths;

  private FileSystem hdfs;

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
    return new String[] { "input-path", "output-path" };
  }

  @Override
  public void reset() {
    super.reset();
    for (RecordCounter counter : COUNTERS) {
      incrementCounter(Process.class.getCanonicalName(), counter, 0);
    }
  }

  @Override
  public int prepare(String... arguments) throws Exception {
    if (arguments == null || arguments.length != 2) {
      throw new Exception("Invalid number of arguments");
    }
    hdfs = FileSystem.newInstance(getConf());
    inputPath = new Path(arguments[0]);
    if (LOG.isInfoEnabled()) {
      LOG.info("Input path [" + inputPath + "] validated");
    }
    inputPaths = DfsUtil.listDirs(hdfs, inputPath, true, true);
    outputPath = new Path(arguments[1]);
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
      job.setJarByClass(Process.class);
      job.getConfiguration().set(Constants.CONFIG_INPUT_PATH, inputPath.toString());
      job.getConfiguration().set(Constants.CONFIG_OUTPUT_PATH, outputPath.toString());
      job.getConfiguration().set(FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, Boolean.FALSE.toString());
      for (Path inputPath : inputPaths) {
        MultipleInputs.addInputPath(job, inputPath, RecordFactory
            .getRecordSequenceInputFormat(inputPath.getParent().getParent().getParent().getParent().getName()));
      }
      job.setMapperClass(Mapper.class);
      job.setSortComparatorClass(RecordKey.RecordKeyComparator.class);
      job.setMapOutputKeyClass(RecordKey.class);
      AvroJob.setMapOutputValueSchema(job, Record.getClassSchema());
      job.setReducerClass(Reducer.class);
      job.setNumReduceTasks(inputPaths.size());
      FileOutputFormat.setOutputPath(job, outputPath);
      LazyOutputFormatNoCheck.setOutputFormatClass(job, TextOutputFormat.class);
      AvroJob.setOutputKeySchema(job, Schema.create(Type.NULL));
      AvroJob.setOutputValueSchema(job, Record.getClassSchema());
      MultipleOutputs.addNamedOutput(job, OUTPUT_SEQUENCE, SequenceFileOutputFormat.class, RecordKey.class, Text.class);
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
      extends org.apache.hadoop.mapreduce.Mapper<RecordKey, Record, RecordKey, AvroValue<Record>> {

    private final RecordKey recordKey = new RecordKey();
    private final Record recordValue = new Record();
    private final Text textValue = new Text();
    private final StringBuilder string = new StringBuilder(512);
    private final AvroValue<Record> recordWrapped = new AvroValue<Record>();

    private MultipleOutputs<RecordKey, Text> multipleOutputs;

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected void setup(
        org.apache.hadoop.mapreduce.Mapper<RecordKey, Record, RecordKey, AvroValue<Record>>.Context context)
            throws IOException, InterruptedException {
      multipleOutputs = new MultipleOutputs(context);
    }

    @Override
    protected void cleanup(
        org.apache.hadoop.mapreduce.Mapper<RecordKey, Record, RecordKey, AvroValue<Record>>.Context context)
            throws IOException, InterruptedException {
      multipleOutputs.close();
    }

    @Override
    protected void map(RecordKey key, Record value,
        org.apache.hadoop.mapreduce.Mapper<RecordKey, Record, RecordKey, AvroValue<Record>>.Context context)
            throws IOException, InterruptedException {
      if (key.isValid()) {
        key.setHash(recordValue.hashCode());
        recordWrapped.datum(value);
        context.write(recordKey, recordWrapped);
      } else {
        context.getCounter(RecordCounter.RECORDS).increment(1);
        context.getCounter(RecordCounter.RECORDS_MALFORMED).increment(1);
        textValue.set(key.getSource());
        string.setLength(0);
        multipleOutputs.write(OUTPUT_SEQUENCE, key, textValue, string.append(Constants.DIR_DS_MYDATASET_MALFORMED)
            .append(Path.SEPARATOR_CHAR).append(key.getBatch()).toString());
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

    private static final String PARTITION_YEAR = "ingest_batch_year=";
    private static final String PARTITION_MONTH = "ingest_batch_month=";
    private static final String PARTITION_FILE = "mydataset-" + UUID.randomUUID().toString();

    private final AvroKey<Record> record = new AvroKey<Record>();
    private final StringBuilder string = new StringBuilder(512);
    private final Set<AvroValue<Record>> records = new HashSet<AvroValue<Record>>(32);
    private final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

    private AvroMultipleOutputs multipleOutputsAvro;

    @Override
    protected void setup(
        org.apache.hadoop.mapreduce.Reducer<RecordKey, AvroValue<Record>, NullWritable, AvroValue<Record>>.Context context)
            throws IOException, InterruptedException {
      multipleOutputsAvro = new AvroMultipleOutputs(context);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
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
        RecordCounter counter = records.add(record) ? RecordCounter.RECORDS_CLEANSED : RecordCounter.RECORDS_DUPLICATE;
        context.getCounter(counter).increment(1);
        this.record.datum(record.datum());
        calendar.setTimeInMillis(record.datum().getMyTimestamp());
        string.setLength(0);
        multipleOutputsAvro.write(OUTPUT_AVRO, this.record, NullWritable.get(),
            string
                .append(counter.equals(RecordCounter.RECORDS_CLEANSED)
                    ? Constants.DIR_DS_MYDATASET_PROCESSED_CLEANSED_AVRO_RELATIVE
                    : Constants.DIR_DS_MYDATASET_PROCESSED_DUPLICATE_AVRO_RELATIVE)
                .append(Path.SEPARATOR_CHAR).append(PARTITION_YEAR).append(calendar.get(Calendar.YEAR))
                .append(Path.SEPARATOR_CHAR).append(PARTITION_MONTH).append(calendar.get(Calendar.MONTH) + 1)
                .append(Path.SEPARATOR_CHAR).append(PARTITION_FILE).toString());
      }
    }

  }

  public static void main(String... arguments) throws Exception {
    System.exit(new Process().runner(arguments));
  }

}
