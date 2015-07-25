package com.cloudera.example.model;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Define record source types
 */
public enum RecordType {

  TEXT_TAB(".tsv", "\\t"), TEXT_COMMA(".csv", ",");

  private static final int BYTES_TYPICAL_LENGTH = 512;
  private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

  private String suffix;
  private String delimiter;

  private RecordType(String suffix) {
    this.suffix = suffix;
  }

  private RecordType(String suffix, String delimiter) {
    this.suffix = suffix;
    this.delimiter = delimiter;
  }

  public String getSuffix() {
    return suffix;
  }

  public String getDelimiter() {
    return delimiter;
  }

  /**
   * Serialise a <code>record</code>.<br>
   * <br>
   * Note that this implementation is not thread safe.
   *
   * @param record
   *          the record to serialise
   * @param name
   *          the name of the source of the record
   * @param value
   *          the value of the source of the record
   * @return <code>true</code> if successful, <code>false</code> otherwise
   */
  public String serialise(Record record) {
    return new StringBuilder(BYTES_TYPICAL_LENGTH).append(DATE_FORMAT.format(new Date(record.getMyTimestamp())))
        .append(getDelimiter()).append(record.getMyInteger()).append(getDelimiter()).append(record.getMyDouble())
        .append(getDelimiter()).append(record.getMyBoolean()).append(getDelimiter()).append(record.getMyString())
        .toString();
  }

  /**
   * Deserialise a <code>value</code> from <code>name</code> into a
   * <code>record</code>.<br>
   * <br>
   * Note that this implementation is not thread safe and if parse fails, the
   * <code>record</code> will be inconsistent and may have been partially
   * modified.
   *
   * @param record
   *          the record to deserialise into
   * @param name
   *          the name of the source of the record
   * @param value
   *          the value of the source of the record
   * @return <code>true</code> if successful, <code>false</code> otherwise
   */
  public static boolean deserialise(Record record, String name, String value) {
    String[] values = null;
    if (name.endsWith(TEXT_TAB.getSuffix())) {
      // Note that String.split() implementation is efficient given a single
      // character length split string, which it chooses not to use as a regex
      values = value.split(TEXT_TAB.getDelimiter());
    } else if (name.endsWith(TEXT_COMMA.getSuffix())) {
      // Ditto as above
      values = value.split(TEXT_COMMA.getDelimiter());
    }
    boolean valid = values != null && values.length == Record.SCHEMA$.getFields().size();
    if (valid) {
      try {
        // Use static DateFormat, this is not threadsafe!
        record.setMyTimestamp(DATE_FORMAT.parse(values[0]).getTime());
        record.setMyInteger(Integer.parseInt(values[1]));
        record.setMyDouble(Double.parseDouble(values[2]));
        record.setMyBoolean(Boolean.parseBoolean(values[3]));
        record.setMyString(values[4]);
      } catch (Exception exception) {
        // This exception branch is expensive, but assume this is rare
        valid = false;
      }
    }
    return valid;
  }

}
