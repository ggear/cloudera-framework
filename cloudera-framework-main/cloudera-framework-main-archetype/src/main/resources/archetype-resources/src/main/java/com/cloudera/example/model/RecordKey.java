package com.cloudera.example.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema.Field;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * Store meta-data against {@link Record records}, allowing efficient equality
 * and comparison for sorting
 */
public class RecordKey implements WritableComparable<RecordKey> {

  public static final int FIELDS_NUMBER;

  static {
    int recordFieldsNumber = 0;
    for (Field field : Record.SCHEMA$.getFields()) {
      if (field.order() != Field.Order.IGNORE) {
        recordFieldsNumber++;
      }
    }
    FIELDS_NUMBER = recordFieldsNumber;
  }

  private static final Pattern REGEX_PATH = Pattern.compile(
      ".*/?(([a-zA-Z0-9\\-]*)/([a-zA-Z0-9\\-]*)/([a-zA-Z0-9\\-]*)/ingest_batch_name=([1-9][0-9]{12})_?([1-9][0-9]{12})?_mydataset([a-zA-Z0-9\\-]*)\\.([a-z]+)\\.?([a-z]*)/([1-9][0-9]{12})_?([1-9][0-9]{12})?_mydataset([a-zA-Z0-9\\-\\.]*)\\.(.*))");

  private int hash;
  private String type;
  private String codec;
  private String container;
  private long timestamp;
  private String batch;
  private String source;
  private boolean valid;

  public RecordKey() {
  }

  public RecordKey(int hash, String batch) {
    this.hash = hash;
    this.batch = batch;
    this.valid = false;
    if (batch != null) {
      Matcher matcher = REGEX_PATH.matcher(batch);
      if (matcher.matches()) {
        try {
          this.type = matcher.group(3);
          this.codec = matcher.group(4);
          this.container = matcher.group(2);
          this.timestamp = Long.parseLong(matcher.group(10));
          this.batch = matcher.group(1);
          this.valid = true;
        } catch (Exception exception) {
          // ignore
        }
      }
    }
  }

  public void set(RecordKey that) {
    this.hash = that.hash;
    this.type = that.type;
    this.codec = that.codec;
    this.container = that.container;
    this.timestamp = that.timestamp;
    this.batch = that.batch;
    this.source = that.source;
    this.valid = that.valid;
  }

  public int getHash() {
    return hash;
  }

  public void setHash(int hash) {
    this.hash = hash;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getCodec() {
    return codec;
  }

  public void setCodec(String codec) {
    this.codec = codec;
  }

  public String getContainer() {
    return container;
  }

  public void setContainer(String container) {
    this.container = container;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public String getBatch() {
    return batch;
  }

  public void setBatch(String batch) {
    this.batch = batch;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public boolean isValid() {
    return valid;
  }

  public void setValid(boolean valid) {
    this.valid = valid;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, hash);
    WritableUtils.writeString(out, type);
    WritableUtils.writeString(out, codec);
    WritableUtils.writeString(out, container);
    WritableUtils.writeVLong(out, timestamp);
    WritableUtils.writeString(out, batch);
    WritableUtils.writeVInt(out, valid ? 1 : 0);

  }

  @Override
  public void readFields(DataInput in) throws IOException {
    hash = WritableUtils.readVInt(in);
    type = WritableUtils.readString(in);
    codec = WritableUtils.readString(in);
    container = WritableUtils.readString(in);
    timestamp = WritableUtils.readVLong(in);
    batch = WritableUtils.readString(in);
    valid = WritableUtils.readVInt(in) == 0 ? false : true;
  }

  @Override
  public int compareTo(RecordKey that) {
    return Integer.compare(this.hash, that.hash);
  }

  @Override
  public String toString() {
    return "RecordKey [hash=" + hash + ", type=" + type + ", codec=" + codec + ", container=" + container
        + ", timestamp=" + timestamp + ", batch=" + batch + ", source=" + source + ", valid=" + valid + "]";
  }

  @Override
  public int hashCode() {
    return hash;
  }

  @Override
  public boolean equals(Object that) {
    if (this == that)
      return true;
    if (that == null)
      return false;
    if (getClass() != that.getClass())
      return false;
    return this.hash == ((RecordKey) that).hash;
  }

  public static class RecordKeyComparator extends WritableComparator {

    public RecordKeyComparator() {
      super(RecordKey.class);
    }

    @Override
    public int compare(byte[] bytes1, int start1, int length1, byte[] bytes2, int start2, int length2) {
      return Integer.compare(readInt(bytes1, start1), readInt(bytes2, start2));
    }

  }

}
