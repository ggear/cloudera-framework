package com.cloudera.framework.example.one.model;

import java.io.IOException;
import java.util.Map;

import com.cloudera.framework.example.one.model.input.RecordSequenceInputFormatCsv;
import com.cloudera.framework.example.one.model.input.RecordSequenceInputFormatXml;
import com.cloudera.framework.example.one.model.serde.RecordStringSerDe;
import com.cloudera.framework.example.one.model.serde.RecordStringSerDeCsv;
import com.cloudera.framework.example.one.model.serde.RecordStringSerDeXml;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

/**
 * Factory to access {@link Record} related instances
 */
public class RecordFactory {

  public static final String RECORD_STRING_SERDE_CSV = "csv";
  public static final String RECORD_STRING_SERDE_XML = "xml";

  private static final Map<String, ? extends RecordStringSerDe> RECORD_STRING_SERDES = ImmutableMap.of(RECORD_STRING_SERDE_CSV,
    new RecordStringSerDeCsv(), RECORD_STRING_SERDE_XML, new RecordStringSerDeXml());

  private static final Map<String, Class<? extends SequenceFileInputFormat<RecordKey, AvroGenericRecordWritable>>> RECORD_INPUT_FORMATS =
    ImmutableMap
      .of(RECORD_STRING_SERDE_CSV, RecordSequenceInputFormatCsv.class, RECORD_STRING_SERDE_XML, RecordSequenceInputFormatXml.class);

  /**
   * Get a {@link RecordStringSerDe} from a <code>type</code>
   *
   * @param type the type to lookup
   * @return the {@link RecordStringSerDe} instance
   */
  public static RecordStringSerDe getRecordStringSerDe(String type) throws IOException {
    if (!RECORD_STRING_SERDES.containsKey(type)) {
      throw new IOException("Could not find [RecordStringSerDe] for type [" + type + "]");
    }
    return RECORD_STRING_SERDES.get(type);
  }

  /**
   * Get a {@link SequenceFileInputFormat} from a <code>type</code>
   *
   * @param type the type to lookup
   * @return the {@link SequenceFileInputFormat} instance
   */
  public static Class<? extends SequenceFileInputFormat<RecordKey, AvroGenericRecordWritable>> getRecordSequenceInputFormat(String type)
    throws IOException {
    if (!RECORD_INPUT_FORMATS.containsKey(type)) {
      throw new IOException("Could not find [RecordSequenceInputFormat] for type [" + type + "]");
    }
    return RECORD_INPUT_FORMATS.get(type);
  }

}
