package com.cloudera.framework.common.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Provide MR utility functions
 */

public class MrUtil {

  public static final String CODEC_NONE = "none";

  /**
   * Get the codec string associated with this <code>configuration</code>
   *
   * @param configuration
   *          the {@link Configuration}
   * @return the codec {@link String}
   */
  public static String getCodecString(Configuration configuration) {
    boolean compress = configuration.getBoolean(FileOutputFormat.COMPRESS, false);
    String codecType = configuration.get(FileOutputFormat.COMPRESS_TYPE, null);
    if (compress && (codecType == null || !codecType.equals(CompressionType.NONE.toString()))) {
      Class<?> codecClass = configuration.getClass(FileOutputFormat.COMPRESS_CODEC, DefaultCodec.class);
      if (codecClass == null) {
        return CODEC_NONE;
      } else {
        try {
          return ((CompressionCodec) codecClass.newInstance()).getDefaultExtension().replace(".", "");
        } catch (Exception exception) {
          throw new RuntimeException("Could not determine codec", exception);
        }
      }
    }
    return CODEC_NONE;
  }

}
