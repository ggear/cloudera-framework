package com.cloudera.example.model.serde;

import java.io.IOException;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import com.cloudera.example.model.Record;
import com.cloudera.example.model.RecordKey;

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVWriter;

/**
 * CSV {@link RecordStringSerDe} implementation.<br>
 * <br>
 * This class is not thread-safe.
 */
public class RecordStringSerDeCsv extends RecordStringSerDe {

  private static final DateFormat FIELD_DATE = new SimpleDateFormat("yyyy-MM-dd");

  private static final String RECORD_DELIM = "\n";
  private static final int RECORD_TYPICAL_SIZE = 512;

  private static final char FIELD_DELIM = ',';
  private static final char FIELD_QUOTE = '"';
  private static final char FIELD_ESCAPE = '\\';
  private static final CSVParser FIELD_DESERIALISER = new CSVParser(FIELD_DELIM, FIELD_QUOTE, FIELD_ESCAPE, false,
      true);

  @Override
  public RecordStringDe getDeserialiser(final RecordKey recordKey, final Record record, final String string) {
    return new RecordStringDe() {

      private int index = -1;
      private String[] records;

      private void initialise() {
        if (records == null) {
          // Note that the split() implementation is efficient given a single
          // char length split string, since regex is not used. A one pass
          // string algorithm would be more efficient, but we require the String
          // tokens to process with CVSParser, so again, this is OK
          records = string.split(RECORD_DELIM);
        }
      }

      @Override
      public boolean hasNext() {
        initialise();
        return index + 1 < records.length;
      }

      @Override
      public boolean next(RecordKey recordsKey) throws IOException {
        initialise();
        recordKey.set(recordsKey);
        String[] fields = FIELD_DESERIALISER.parseLine(records[++index]);
        recordKey.setValid(recordKey.isValid() && fields != null && fields.length == RecordKey.FIELDS_NUMBER);
        if (recordKey.isValid()) {
          record.setIngestId(UUID.randomUUID().toString());
          record.setIngestTimestamp(recordKey.getTimestamp());
          record.setIngestBatch(recordKey.getBatch());
          try {
            record.setMyTimestamp(FIELD_DATE.parse(fields[0]).getTime());
          } catch (Exception exception) {
            recordKey.setValid(false);
            record.setMyTimestamp(null);
          }
          try {
            record.setMyInteger(Integer.parseInt(fields[1]));
          } catch (Exception exception) {
            recordKey.setValid(false);
            record.setMyInteger(null);
          }
          try {
            record.setMyDouble(Double.parseDouble(fields[2]));
          } catch (Exception exception) {
            recordKey.setValid(false);
            record.setMyDouble(null);
          }
          try {
            record.setMyBoolean(Boolean.parseBoolean(fields[3]));
          } catch (Exception exception) {
            recordKey.setValid(false);
            record.setMyBoolean(null);
          }
          record.setMyString(fields[4]);
        }
        if (!recordKey.isValid()) {
          recordKey.setSource(records[index]);
        }
        return recordKey.isValid();
      }

    };

  }

  @Override
  public RecordStringSer getSerialiser(final int size) {
    return new RecordStringSer() {

      private StringWriter string = null;
      private CSVWriter serialiser = null;

      private void initialise() {
        if (serialiser == null) {
          serialiser = new CSVWriter(string = new StringWriter(size * RECORD_TYPICAL_SIZE), FIELD_DELIM,
              CSVWriter.NO_QUOTE_CHARACTER, FIELD_ESCAPE, RECORD_DELIM);
        }
      }

      @Override
      public void add(Record record) {
        initialise();
        serialiser.writeNext(
            new String[] { record.getMyTimestamp() == null ? "" : FIELD_DATE.format(new Date(record.getMyTimestamp())),
                "" + record.getMyInteger(), "" + record.getMyDouble(), "" + record.getMyBoolean(),
                "" + record.getMyString() });
      }

      @Override
      public String get() {
        initialise();
        return string.toString();
      }

    };

  }

}
