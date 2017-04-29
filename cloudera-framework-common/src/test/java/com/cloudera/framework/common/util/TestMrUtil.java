package com.cloudera.framework.common.util;

import static org.junit.Assert.assertEquals;

import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(TestRunner.class)
public class TestMrUtil {

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  @Test
  public void testGetCodecString() {
    Configuration configuration = dfsServer.getConf();
    assertEquals(MrUtil.CODEC_NONE, MrUtil.getCodecString(configuration));
    configuration.setBoolean(FileOutputFormat.COMPRESS, false);
    assertEquals(MrUtil.CODEC_NONE, MrUtil.getCodecString(configuration));
    configuration.setBoolean(FileOutputFormat.COMPRESS, true);
    assertEquals(new DefaultCodec().getDefaultExtension().substring(1, new DefaultCodec().getDefaultExtension().length()),
      MrUtil.getCodecString(configuration));
    configuration.set(FileOutputFormat.COMPRESS_CODEC, SnappyCodec.class.getName());
    assertEquals(new SnappyCodec().getDefaultExtension().substring(1, new SnappyCodec().getDefaultExtension().length()),
      MrUtil.getCodecString(configuration));
    configuration.set(FileOutputFormat.COMPRESS_TYPE, CompressionType.BLOCK.toString());
    assertEquals(new SnappyCodec().getDefaultExtension().substring(1, new SnappyCodec().getDefaultExtension().length()),
      MrUtil.getCodecString(configuration));
    configuration.set(FileOutputFormat.COMPRESS_TYPE, CompressionType.NONE.toString());
    assertEquals(MrUtil.CODEC_NONE, MrUtil.getCodecString(configuration));
    configuration.set(FileOutputFormat.COMPRESS_TYPE, CompressionType.BLOCK.toString());
    configuration.setBoolean(FileOutputFormat.COMPRESS, false);
    assertEquals(MrUtil.CODEC_NONE, MrUtil.getCodecString(configuration));
  }

}
