package com.cloudera.example.model.input;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import com.cloudera.example.Constants;
import com.cloudera.example.model.RecordKey;

/**
 * An {@link InputFormat} to act on multiple text files, forming the appropriate
 * {@link RecordKey key} and UTF8 parsed {@link Text value}.
 */
public class RecordTextInputFormat extends CombineFileInputFormat<RecordKey, Text> {

  public static int SPLIT_SIZE_BYTES_MAX = 128 * 1024 * 1024;

  public RecordTextInputFormat() {
    setMaxSplitSize(SPLIT_SIZE_BYTES_MAX);
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  @Override
  public RecordReader<RecordKey, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException {
    return new CombineFileRecordReader<RecordKey, Text>((CombineFileSplit) split, context, RecordReaderText.class);
  }

  public static class RecordReaderText extends RecordReader<RecordKey, Text> {

    private static final Log LOG = LogFactory.getLog(RecordReaderText.class);

    private Path path;
    private Text value;
    private RecordKey key;
    private String inputPath;
    private InputStream stream;

    public RecordReaderText(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException {
      path = split.getPath(index);
      inputPath = context.getConfiguration().get(Constants.CONFIG_INPUT_PATH);
      if (inputPath == null) {
        throw new IOException("Configuration [" + Constants.CONFIG_INPUT_PATH + "] expected but not found");
      }
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      try {
        stream = new BufferedInputStream(path.getFileSystem(context.getConfiguration()).open(path));
      } catch (IOException exception) {
        if (LOG.isErrorEnabled()) {
          LOG.error("Could not read file [" + path + "]", exception);
        }
      }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (stream != null) {
        try {
          String valueString = IOUtils.toString(stream, Charsets.UTF_8.name());
          value = new Text(valueString);
          String pathString = path.toString();
          try {
            pathString = pathString.substring(pathString.indexOf(inputPath) + inputPath.length() + 1,
                pathString.length());
          } catch (IndexOutOfBoundsException exception) {
            // ignore
          }
          key = new RecordKey(valueString.hashCode(), pathString);
          return true;
        } catch (IOException exception) {
          if (LOG.isErrorEnabled()) {
            LOG.error("Could not read file [" + path + "]", exception);
          }
        } finally {
          stream.close();
          stream = null;
        }
      }
      return false;
    }

    @Override
    public RecordKey getCurrentKey() throws IOException, InterruptedException {
      return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
      return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return stream == null ? 1F : 0F;
    }

    @Override
    public void close() throws IOException {
      if (stream != null) {
        stream.close();
        stream = null;
      }
    }

  }

}
