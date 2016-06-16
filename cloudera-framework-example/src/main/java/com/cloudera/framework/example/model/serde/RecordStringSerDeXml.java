package com.cloudera.framework.example.model.serde;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;

import com.cloudera.framework.example.model.Record;
import com.cloudera.framework.example.model.RecordKey;

/**
 * An XML {@link RecordStringSerDe} implementation.
 */
public class RecordStringSerDeXml extends RecordStringSerDe {

  private static final int RECORD_TYPICAL_SIZE = 1024;

  private JAXBContext jaxbContext;

  public RecordStringSerDeXml() {
    try {
      jaxbContext = JAXBContext.newInstance(RecordsXml.class);
    } catch (JAXBException exception) {
      throw new RuntimeException("Could not initialise JAXB", exception);
    }
  }

  @Override
  public RecordStringDe getDeserialiser(final RecordKey recordKey, final Record record, final String string) {
    return new RecordStringDe() {

      private int index = -1;
      private boolean corrupt;
      private List<Record> records;

      private void initialise() throws IOException {
        if (records == null && !corrupt) {
          try {
            records = ((RecordsXml) jaxbContext.createUnmarshaller()
                .unmarshal(IOUtils.toInputStream(string.toString(), Charset.forName(Charsets.UTF_8.name())))).get();
          } catch (Exception exception) {
            corrupt = true;
          }
        }
      }

      @Override
      public boolean hasNext() throws IOException {
        initialise();
        return index == -1 || records != null && index + 1 < records.size();
      }

      @Override
      public boolean next(RecordKey recordsKey) throws IOException {
        initialise();
        recordKey.set(recordsKey);
        recordKey.setValid(recordKey.isValid() && records != null && !corrupt);
        index++;
        if (recordKey.isValid()) {
          record.setIngestId(UUID.randomUUID().toString());
          record.setIngestTimestamp(recordKey.getTimestamp());
          record.setIngestBatch(recordKey.getBatch());
          record.setMyTimestamp(records.get(index).getMyTimestamp());
          record.setMyInteger(records.get(index).getMyInteger());
          record.setMyDouble(records.get(index).getMyDouble());
          record.setMyBoolean(records.get(index).getMyBoolean());
          record.setMyString(records.get(index).getMyString());
        } else {
          recordKey.setSource(string);
        }
        return recordKey.isValid();
      }

    };

  }

  @Override
  public RecordStringSer getSerialiser(final int size) {
    return new RecordStringSer() {

      private RecordsXml records;
      private Marshaller serialiser;

      private void initialise() throws IOException {
        if (records == null) {
          try {
            records = new RecordsXml(size);
            serialiser = jaxbContext.createMarshaller();
            serialiser.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
          } catch (Exception exception) {
            throw new IOException("Could not initialise JAXB", exception);
          }
        }
      }

      @Override
      public void add(Record record) throws IOException {
        initialise();
        records.add(record);
      }

      @Override
      public String get() throws IOException {
        initialise();
        StringWriter string = new StringWriter(size * RECORD_TYPICAL_SIZE);
        try {
          serialiser.marshal(records, string);
        } catch (Exception exception) {
          throw new IOException("Could not marshall XML", exception);
        }
        return string.toString();
      }

    };

  }

  @SuppressWarnings("unused")
  @XmlRootElement(name = "records")
  @XmlAccessorType(XmlAccessType.NONE)
  private static class RecordsXml {

    @XmlElement(name = "record")
    private List<RecordXml> records;

    public RecordsXml() {
    }

    public RecordsXml(int size) {
      records = new ArrayList<>(size);
    }

    public void add(Record record) {
      records.add(new RecordXml(record));
    }

    public List<Record> get() {
      List<Record> records = new ArrayList<>(this.records == null ? 0 : this.records.size());
      if (this.records != null) {
        for (RecordXml record : this.records) {
          records.add(record.get());
        }
      }
      return records;
    }

  }

  @SuppressWarnings("unused")
  @XmlAccessorType(XmlAccessType.FIELD)
  private static class RecordXml {

    private Long my_timestamp;
    private Integer my_integer;
    private Double my_double;
    private Boolean my_boolean;
    private String my_string;

    public RecordXml() {
    }

    public RecordXml(Record record) {
      my_timestamp = record.getMyTimestamp();
      my_integer = record.getMyInteger();
      my_double = record.getMyDouble();
      my_boolean = record.getMyBoolean();
      my_string = record.getMyString();
    }

    public Record get() {
      return Record.newBuilder().setMyTimestamp(my_timestamp).setMyInteger(my_integer).setMyDouble(my_double).setMyBoolean(my_boolean)
          .setMyString(my_string).build();
    }

  }

}
