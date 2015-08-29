package com.cloudera.example.model.serde;

import java.io.IOException;

import com.cloudera.example.model.Record;
import com.cloudera.example.model.RecordKey;

public abstract class RecordStringSerDe {

  public abstract RecordStringDe getDeserialiser(final RecordKey recordKey, final Record record, final String string);

  public abstract RecordStringSer getSerialiser(final int size);

  /**
   * Deserialise a {@link Record}.<br>
   * <br>
   * Implementations are not required to be thread-safe.
   */
  public interface RecordStringDe {

    public boolean hasNext() throws IOException;

    public boolean next(RecordKey recordsKey) throws IOException;

  }

  /**
   * Serialise a {@link Record}.<br>
   * <br>
   * Implementations are not required to be thread-safe.
   */
  public interface RecordStringSer {

    public void add(Record record) throws IOException;

    public String get() throws IOException;

  }

}
