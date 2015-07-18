package com.cloudera.example.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * Record Key, storing meta-data against {@link Record Records}, allowing
 * efficient sorting and comparison
 */
public class RecordKey implements WritableComparable<RecordKey> {

  private int hash;
  private String path;
  private boolean valid;
  private String source;

  public RecordKey() {
  }

  public RecordKey(int hash, String path, boolean valid, String source) {
    this.hash = hash;
    this.path = path;
    this.valid = valid;
    this.source = source;
  }

  public int getHash() {
    return hash;
  }

  public void setHash(int hash) {
    this.hash = hash;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public boolean isValid() {
    return valid;
  }

  public void setValid(boolean valid) {
    this.valid = valid;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, hash);
    WritableUtils.writeString(out, path);
    WritableUtils.writeVInt(out, valid ? 1 : 0);
    WritableUtils.writeString(out, source);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    hash = WritableUtils.readVInt(in);
    path = WritableUtils.readString(in);
    valid = WritableUtils.readVInt(in) == 0 ? false : true;
    source = WritableUtils.readString(in);
  }

  @Override
  public int compareTo(RecordKey that) {
    return Integer.compare(this.hash, that.hash);
  }

  @Override
  public String toString() {
    return "RecordKey [hash=" + hash + ", path=" + path + ", valid=" + valid + ", source=" + source + "]";
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
