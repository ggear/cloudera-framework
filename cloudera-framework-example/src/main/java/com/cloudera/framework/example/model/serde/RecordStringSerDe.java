package com.cloudera.framework.example.model.serde;

import java.io.IOException;

import com.cloudera.framework.example.model.Record;
import com.cloudera.framework.example.model.RecordKey;

/**
 * A {@link Record} to and from {@link String} serialisation and
 * de-serialisation interface
 */
public abstract class RecordStringSerDe {

  // TODO: Implement in a stream orientated fashion, to allow for efficient
  // large (>100MB) serilisation/de-serialisation jobs, the current interface
  // is optimised for small (<100MB) jobs.

  /**
   * Get a de-serialiser
   *
   * @param recordKey
   * @param record
   * @param string
   * @return
   */
  public abstract RecordStringDe getDeserialiser(final RecordKey recordKey, final Record record, final String string);

  /**
   * Get a serialiser
   *
   * @param size
   * @return
   */
  public abstract RecordStringSer getSerialiser(final int size);

  /**
   * A de-serialiser.<br>
   * <br>
   * Implementations are not required to be thread-safe.
   */
  public interface RecordStringDe {

    /**
     * Determines if the de-serialiser is exhausted
     *
     * @return
     * @throws IOException
     */
    boolean hasNext() throws IOException;

    /**
     * Get the next key and record
     *
     * @param recordsKey
     * @return
     * @throws IOException
     */
    boolean next(RecordKey recordsKey) throws IOException;

  }

  /**
   * A serialiser.<br>
   * <br>
   * Implementations are not required to be thread-safe.
   */
  public interface RecordStringSer {

    /**
     * Add a record
     *
     * @param record
     * @throws IOException
     */
    void add(Record record) throws IOException;

    /**
     * Get the string
     *
     * @return
     * @throws IOException
     */
    String get() throws IOException;

  }

}
