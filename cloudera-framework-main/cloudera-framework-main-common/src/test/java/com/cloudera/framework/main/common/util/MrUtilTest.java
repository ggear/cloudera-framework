package com.cloudera.framework.main.common.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Assert;
import org.junit.Test;

import com.cloudera.framework.main.test.LocalClusterDfsMrTest;

public class MrUtilTest extends LocalClusterDfsMrTest {

  @Test
  public void testGetCodecString() {
    Configuration configuration = getConf();
    Assert.assertEquals(MrUtil.CODEC_NONE, MrUtil.getCodecString(configuration));
    configuration.setBoolean(FileOutputFormat.COMPRESS, false);
    Assert.assertEquals(MrUtil.CODEC_NONE, MrUtil.getCodecString(configuration));
    configuration.setBoolean(FileOutputFormat.COMPRESS, true);
    Assert.assertEquals(
        new DefaultCodec().getDefaultExtension().substring(1, new DefaultCodec().getDefaultExtension().length()),
        MrUtil.getCodecString(configuration));
    configuration.set(FileOutputFormat.COMPRESS_CODEC, SnappyCodec.class.getName());
    Assert.assertEquals(
        new SnappyCodec().getDefaultExtension().substring(1, new SnappyCodec().getDefaultExtension().length()),
        MrUtil.getCodecString(configuration));
    configuration.set(FileOutputFormat.COMPRESS_TYPE, CompressionType.BLOCK.toString());
    Assert.assertEquals(
        new SnappyCodec().getDefaultExtension().substring(1, new SnappyCodec().getDefaultExtension().length()),
        MrUtil.getCodecString(configuration));
    configuration.set(FileOutputFormat.COMPRESS_TYPE, CompressionType.NONE.toString());
    Assert.assertEquals(MrUtil.CODEC_NONE, MrUtil.getCodecString(configuration));
    configuration.set(FileOutputFormat.COMPRESS_TYPE, CompressionType.BLOCK.toString());
    configuration.setBoolean(FileOutputFormat.COMPRESS, false);
    Assert.assertEquals(MrUtil.CODEC_NONE, MrUtil.getCodecString(configuration));
  }

}
