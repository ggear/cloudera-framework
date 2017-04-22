package com.cloudera.framework.example.model.input;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.cloudera.framework.example.Constants;
import com.cloudera.framework.example.model.RecordKey;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * An {@link CombineFileInputFormat} to act on text files, forming the
 * {@link RecordKey RecordKeys} and {@link Text} values
 */
public class RecordTextCombineInputFormat extends CombineFileInputFormat<RecordKey, Text> {

  public static final int SPLIT_SIZE_BYTES_MAX = 128 * 1024 * 1024;

  public RecordTextCombineInputFormat() {
    setMaxSplitSize(SPLIT_SIZE_BYTES_MAX);
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  @Override
  public RecordReader<RecordKey, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
    return new CombineFileRecordReader<>((CombineFileSplit) split, context, RecordReaderText.class);
  }

  public static class RecordReaderText extends RecordReader<RecordKey, Text> {

    private static final Log LOG = LogFactory.getLog(RecordReaderText.class);

    private Path path;
    private Text value;
    private RecordKey key;
    private String inputPath;
    private InputStream stream;

    public RecordReaderText(InputSplit split, TaskAttemptContext context) throws IOException {
      this(split, context, null);
    }

    public RecordReaderText(InputSplit split, TaskAttemptContext context, Integer index) throws IOException {
      if (split instanceof CombineFileSplit) {
        path = ((CombineFileSplit) split).getPath(index);
      } else if (split instanceof FileSplit) {
        path = ((FileSplit) split).getPath();
      } else {
        throw new IOException("Could not determine source file from split [" + split.getClass().getName() + "]");
      }
      inputPath = context.getConfiguration().get(Constants.CONFIG_INPUT_PATH);
      if (inputPath == null) {
        throw new IOException("Configuration [" + Constants.CONFIG_INPUT_PATH + "] expected but not found");
      }
    }

    public RecordReaderText(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException {
      this((InputSplit) split, context, index);
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
            pathString = pathString.substring(pathString.indexOf(inputPath) + inputPath.length() + 1, pathString.length());
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
