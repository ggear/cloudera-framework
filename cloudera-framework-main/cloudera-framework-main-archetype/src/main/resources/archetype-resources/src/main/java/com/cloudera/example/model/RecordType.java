package com.cloudera.example.model;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Define {@link Record record} source types
 */
public enum RecordType {

  TEXT_TSV("tsv", "\\t"), //
  TEXT_CSV("csv", ",");

  private static final int BYTES_TYPICAL_LENGTH = 512;
  private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
  private static final Map<String, RecordType> RECORD_TYPES_BY_QUALIFIER = new HashMap<String, RecordType>();

  static {
    for (RecordType type : values()) {
      RECORD_TYPES_BY_QUALIFIER.put(type.getQualifier(), type);
    }
  }

  private String qualifier;
  private String delimiter;

  private RecordType(String qualifier) {
    this.qualifier = qualifier;
  }

  private RecordType(String qualifier, String delimiter) {
    this.qualifier = qualifier;
    this.delimiter = delimiter;
  }

  public String getQualifier() {
    return qualifier;
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

  public String[] recordise(String string) {
    String[] strings = new String[0];
    try {
      // Note that String.split() implementation is efficient given a single
      // character length split string, which it chooses not to use as a regex
      strings = string.split("\n");
    } catch (Exception exception) {
      // ignore
    }
    return strings;
  }

  /**
   * Deserialise a <code>string</code> into a <code>record</code>.<br>
   * <br>
   * Note that this implementation is not thread safe and if parse fails, the
   * <code>record</code> will be inconsistent and may have been partially
   * modified.
   *
   * @param record
   *          the record to deserialise into
   * @param string
   *          the {@link String} serialised record
   * @return <code>true</code> if successful, <code>false</code> otherwise
   */
  public boolean deserialise(Record record, String string) {
    boolean valid = true;
    try {
      // Note that String.split() implementation is efficient given a single
      // character length split string, which it chooses not to use as a regex
      String[] strings = string.split(getDelimiter());
      valid = strings != null && strings.length == Record.SCHEMA$.getFields().size();
      if (valid) {
        // Use static DateFormat, this is not threadsafe!
        record.setMyTimestamp(DATE_FORMAT.parse(strings[0]).getTime());
        record.setMyInteger(Integer.parseInt(strings[1]));
        record.setMyDouble(Double.parseDouble(strings[2]));
        record.setMyBoolean(Boolean.parseBoolean(strings[3]));
        record.setMyString(strings[4]);
      }
    } catch (Exception exception) {
      // This exception branch is expensive, but assume this is rare
      valid = false;
    }
    return valid;
  }

  /**
   * 
   * @param type
   * @return
   */
  public static RecordType valueOfQualifier(String qualifier) {
    return RECORD_TYPES_BY_QUALIFIER.get(qualifier);
  }

}
