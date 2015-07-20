package com.cloudera.example.process;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TimeZone;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormatNoCheck;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.example.Constants;
import com.cloudera.example.Constants.Counter;
import com.cloudera.example.model.Record;
import com.cloudera.example.model.RecordKey;
import com.cloudera.framework.main.common.Driver;

/**
 * Cleanse driver, take comma and tab delimitered fields in text files,
 * validate, cleanse and de-dupe them, rewriting into a canonical, partitioned
 * row Avro format as per {@link Record#getClassSchema() schema} and filtering
 * off erroneous records, retaining their original source format
 */
public class Cleanse extends Driver {

  protected static final String OUTPUT_TEXT = "text";
  protected static final String OUTPUT_AVRO = "avro";

  private static final Logger LOG = LoggerFactory.getLogger(Cleanse.class);

  private Set<Path> inputPaths;
  private Path outputPath;

  public Cleanse() {
    super();
  }

  public Cleanse(Configuration confguration) {
    super(confguration);
  }

  @Override
  public String description() {
    return "Clense my dataset";
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
    for (Counter counter : Counter.values()) {
      incrementCounter(Cleanse.class.getCanonicalName(), counter, 0);
    }
  }

  @Override
  public int prepare(String... arguments) throws Exception {
    if (arguments == null || arguments.length != 2) {
      throw new Exception("Invalid number of arguments");
    }
    FileSystem hdfs = FileSystem.newInstance(getConf());
    Path inputPath = new Path(arguments[0]);
    if (!hdfs.exists(inputPath)) {
      throw new Exception("Input path [" + inputPath + "] does not exist");
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("Input path [" + inputPath + "] validated");
    }
    inputPaths = new HashSet<Path>();
    RemoteIterator<LocatedFileStatus> inputPathsIterator = hdfs.listFiles(inputPath, true);
    while (inputPathsIterator.hasNext()) {
      inputPaths.add(inputPathsIterator.next().getPath().getParent());
    }
    outputPath = new Path(arguments[1]);
    hdfs.mkdirs(outputPath.getParent());
    if (hdfs.exists(outputPath)) {
      throw new Exception("Output path [" + inputPath + "] already exists");
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("Output path [" + outputPath + "] validated");
    }
    return RETURN_SUCCESS;
  }

  @Override
  public int execute() throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName(getClass().getSimpleName());
    job.setJarByClass(Cleanse.class);
    for (Path inputPath : inputPaths) {
      FileInputFormat.addInputPath(job, inputPath);
    }
    FileOutputFormat.setOutputPath(job, outputPath);
    LazyOutputFormatNoCheck.setOutputFormatClass(job, TextOutputFormat.class);
    job.getConfiguration().set(FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, Boolean.FALSE.toString());
    job.setSortComparatorClass(RecordKey.RecordKeyComparator.class);
    job.setMapperClass(CleanseMapper.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapOutputKeyClass(RecordKey.class);
    job.setMapOutputValueClass(AvroValue.class);
    AvroJob.setMapOutputValueSchema(job, Record.getClassSchema());
    job.setReducerClass(CleanseReducer.class);
    job.setNumReduceTasks(inputPaths.size());
    AvroJob.setOutputKeySchema(job, Schema.create(Type.NULL));
    AvroJob.setOutputValueSchema(job, Record.getClassSchema());
    MultipleOutputs.addNamedOutput(job, OUTPUT_TEXT, TextOutputFormat.class, NullWritable.class, Text.class);
    AvroMultipleOutputs.addNamedOutput(job, OUTPUT_AVRO, AvroKeyOutputFormat.class, Record.getClassSchema());
    boolean jobSuccess = job.waitForCompletion(LOG.isInfoEnabled());
    importCounters(job, new Counter[] { Counter.RECORDS_MALFORMED, Counter.RECORDS_DUPLICATE, Counter.RECORDS_CLEANSED,
        Counter.RECORDS });
    return jobSuccess ? RETURN_SUCCESS : RETURN_FAILURE_RUNTIME;
  }

  /**
   * Cleanse mapper, parse both comma and tab separated values into
   * {@link Record Records} keyed by {@link RecordKey RecordKeys}.<br>
   * <br>
   * Note this class is not thread-safe but is jvm-reuse-safe, reusing objects
   * where possible.
   */
  private static class CleanseMapper extends Mapper<LongWritable, Text, RecordKey, AvroValue<Record>> {

    private final Record EMPTY_RECORD = new Record();
    private final String[] EMPTY_STRING_ARRAY = new String[0];

    private final DateFormat DATE_FORMAT = new SimpleDateFormat("YYYY-MM-DD");

    private String splitFileName;
    private String splitFilePathRelative;

    private RecordKey recordKey = new RecordKey();
    private Record recordValue = new Record();
    private AvroValue<Record> recordValueWrapped = new AvroValue<Record>();

    @Override
    protected void setup(Mapper<LongWritable, Text, RecordKey, AvroValue<Record>>.Context context)
        throws IOException, InterruptedException {
      Path splitFilePath = ((FileSplit) context.getInputSplit()).getPath();
      splitFileName = splitFilePath.getName();
      splitFilePathRelative = new StringBuilder(128).append(splitFilePath.getParent().getParent().getParent().getName())
          .append(Path.SEPARATOR_CHAR).append(splitFilePath.getParent().getParent().getName())
          .append(Path.SEPARATOR_CHAR).append(splitFilePath.getParent().getName()).append(Path.SEPARATOR_CHAR)
          .append(splitFileName).toString();
    }

    @Override
    protected void map(LongWritable key, Text value,
        Mapper<LongWritable, Text, RecordKey, AvroValue<Record>>.Context context)
            throws IOException, InterruptedException {
      String valueString = value.toString();
      String[] valueStrings = EMPTY_STRING_ARRAY;
      // Note that String.split() implementation is efficient given a single
      // character length split string, which it chooses not to use as a regex
      if (splitFileName.endsWith(Constants.DIR_DS_MYDATASET_RAW_SOURCE_TEXT_COMMA_SUFFIX)) {
        valueStrings = valueString.split(",");
      } else if (splitFileName.endsWith(Constants.DIR_DS_MYDATASET_RAW_SOURCE_TEXT_TAB_SUFFIX)) {
        valueStrings = valueString.split("\t");
      }
      boolean recordValid = valueStrings.length == Record.SCHEMA$.getFields().size();
      if (recordValid) {
        try {
          recordValue.setMyTimestamp(DATE_FORMAT.parse(valueStrings[0]).getTime());
          recordValue.setMyInteger(Integer.parseInt(valueStrings[1]));
          recordValue.setMyDouble(Double.parseDouble(valueStrings[2]));
          recordValue.setMyBoolean(Boolean.parseBoolean(valueStrings[3]));
          recordValue.setMyString(valueStrings[4]);
        } catch (Exception exception) {
          // This exception branch is expensive, but assume this is rare
          recordValid = false;
        }
      }
      recordKey.setValid(recordValid);
      recordKey.setPath(splitFilePathRelative);
      recordKey.setSource(recordValid ? null : valueString);
      recordKey.setHash(recordValid ? recordValue.hashCode() : valueString.hashCode());
      recordValueWrapped.datum(recordValid ? recordValue : EMPTY_RECORD);
      context.write(recordKey, recordValueWrapped);
    }

  }

  /**
   * Cleanse reducer, write out cleansed, partitioned {@link Record Records},
   * filtering malformed and duplicate records as necessary.<br>
   * <br>
   * Note this class is not thread-safe but is jvm-reuse-safe, reusing objects
   * where possible.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static class CleanseReducer extends Reducer<RecordKey, AvroValue<Record>, NullWritable, AvroValue<Record>> {

    private static final String PARTITION_YEAR = "year=";
    private static final String PARTITION_MONTH = "month=";
    private static final String PARTITION_FILE = "data";

    private MultipleOutputs multipleOutputs;
    private AvroMultipleOutputs multipleOutputsAvro;

    private Text source = new Text();
    private AvroKey<Record> record = new AvroKey<Record>();
    private StringBuilder string = new StringBuilder(512);
    private Set<AvroValue<Record>> records = new HashSet<AvroValue<Record>>(32);
    private Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

    @Override
    protected void setup(Reducer<RecordKey, AvroValue<Record>, NullWritable, AvroValue<Record>>.Context context)
        throws IOException, InterruptedException {
      multipleOutputs = new MultipleOutputs(context);
      multipleOutputsAvro = new AvroMultipleOutputs(context);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      multipleOutputs.close();
      multipleOutputsAvro.close();
    }

    @Override
    protected void reduce(RecordKey key, Iterable<AvroValue<Record>> values,
        Reducer<RecordKey, AvroValue<Record>, NullWritable, AvroValue<Record>>.Context context)
            throws IOException, InterruptedException {
      records.clear();
      Iterator<AvroValue<Record>> valuesIterator = values.iterator();
      while (valuesIterator.hasNext()) {
        AvroValue<Record> record = valuesIterator.next();
        context.getCounter(Counter.RECORDS).increment(1);
        if (key.isValid()) {
          Counter counter = records.add(record) ? Counter.RECORDS_CLEANSED : Counter.RECORDS_DUPLICATE;
          this.record.datum(record.datum());
          calendar.setTimeInMillis(record.datum().getMyTimestamp());
          string.setLength(0);
          multipleOutputsAvro.write(OUTPUT_AVRO, this.record, NullWritable.get(), string
              .append(
                  counter.equals(Counter.RECORDS_CLEANSED) ? Constants.DIR_DS_MYDATASET_PROCESSED_CLEANSED_AVRO_RELATIVE
                      : Constants.DIR_DS_MYDATASET_PROCESSED_DUPLICATE_AVRO_RELATIVE)
              .append(Path.SEPARATOR_CHAR).append(PARTITION_YEAR).append(calendar.get(Calendar.YEAR))
              .append(Path.SEPARATOR_CHAR).append(PARTITION_MONTH).append(calendar.get(Calendar.MONTH) + 1)
              .append(Path.SEPARATOR_CHAR).append(PARTITION_FILE).toString());
          context.getCounter(counter).increment(1);
        } else {
          source.set(key.getSource());
          string.setLength(0);
          multipleOutputs.write(OUTPUT_TEXT, NullWritable.get(), source,
              string.append(Constants.DIR_DS_MYDATASET_MALFORMED).append(Path.SEPARATOR_CHAR).append(key.getPath())
                  .toString());
          context.getCounter(Counter.RECORDS_MALFORMED).increment(1);
        }
      }
    }
  }

}
